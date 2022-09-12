using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Subscriptions;
using Rebus.Transport;
using StackExchange.Redis;

namespace Rebus.Redis
{
    public class RedisTransport:AbstractRebusTransport, ISubscriptionStorage
    {
        private readonly string _sectionKey;
        private readonly ConnectionMultiplexer _connectionMultiplexer;
        private readonly HashSet<string> _registeredSubscriptions = new HashSet<string>();
        private readonly SemaphoreSlim _subscriptionSemaphore = new SemaphoreSlim(1, 1);
        private readonly ILog _log;
        private const string TopicExchangeName = "RebusTopics";
        private const int WaitTimeout = 2000;
        private readonly BlockingCollection<(string topic, RedisValue value)> _messagesToProcess = new BlockingCollection<(string, RedisValue v)>();

        public RedisTransport(string connectionString,  
            string inputQueueAddress,
            IRebusLoggerFactory rebusLoggerFactory) : base(inputQueueAddress)
        {
            _connectionMultiplexer = ConnectionMultiplexer.Connect(connectionString);
            _log = rebusLoggerFactory.GetLogger<RedisTransport>();
        }

        public RedisTransport(ConfigurationOptions connectionOptions, string inputQueueAddress, IRebusLoggerFactory rebusLoggerFactory) : base(inputQueueAddress)
        {
            _connectionMultiplexer = ConnectionMultiplexer.Connect(connectionOptions);
            _log = rebusLoggerFactory.GetLogger<RedisTransport>();
        }

        public override void CreateQueue(string address)
        {
            var topic = EnsureFullyQualifiedName(address);
            _connectionMultiplexer.GetSubscriber().Subscribe(topic, CommonSubscriber);
        }

        public override async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            (string topic, RedisValue value) serializedMessage = default;
            try
            {
                _log.Debug("Waiting for message arrive");
                if (!_messagesToProcess.TryTake(out serializedMessage, WaitTimeout, cancellationToken))
                {
                    _log.Debug("No messages within timeout {0} ms", WaitTimeout);
                    return null;
                }

                _log.Debug("Message received");
            }
            catch (OperationCanceledException ex)
            {
                _log.Debug("Reading from queue was cancelled");
                return (TransportMessage) null;
            }
            catch(Exception ex)
            {
                throw new RebusApplicationException(ex,
                    "Unexpected exception thrown while trying to dequeue Redis Bus");
            }
            if (serializedMessage == default)
                return null;
            try
            {
                return JsonConvert.DeserializeObject<TransportMessage>(serializedMessage.value);
            }
            catch (Exception ex)
            {
                throw new RebusApplicationException(ex,
                    "Unexpected exception thrown while trying to deserialize Redis message");
            }
        }

        protected override async Task SendOutgoingMessages(IEnumerable<OutgoingMessage> outgoingMessages, ITransactionContext context)
        {
            foreach (var outgoingMessage in outgoingMessages)
            {
                var serializedMessage = JsonConvert.SerializeObject(outgoingMessage.TransportMessage);
                await _connectionMultiplexer.GetSubscriber()
                    .PublishAsync(outgoingMessage.DestinationAddress, serializedMessage);
            }
        }

        public Task<string[]> GetSubscriberAddresses(string topic)
        {
            topic = EnsureFullyQualifiedName(topic);
            return Task.FromResult(new[]{ topic });
        }

        private string EnsureFullyQualifiedName(string topic)
        {
            if (!topic.Contains("@"))
                topic = topic + "@" + TopicExchangeName;
            return topic;
        }

        public async Task RegisterSubscriber(string topic, string subscriberAddress)
        {
            var fullyQualifiedTopic = EnsureFullyQualifiedName(topic);
            _log.Debug("Registering subscriber {subscription}", (fullyQualifiedTopic, subscriberAddress));
            await this._subscriptionSemaphore.WaitAsync();
            try
            {
                if (!_registeredSubscriptions.Contains(fullyQualifiedTopic))
                {
                    await _connectionMultiplexer.GetSubscriber().SubscribeAsync(fullyQualifiedTopic,
                        CommonSubscriber);
                    _registeredSubscriptions.Add(fullyQualifiedTopic);

                }
            }
            finally
            {
                this._subscriptionSemaphore.Release();
            }

        }

        private void CommonSubscriber(RedisChannel c, RedisValue v)
        {
            _messagesToProcess.Add((c.ToString(), v));
        }

        public async Task UnregisterSubscriber(string topic, string subscriberAddress)
        {
            var fullyQualifiedTopic = EnsureFullyQualifiedName(topic);
            _log.Debug("Unregistering subscriber {subscription}", (fullyQualifiedTopic, subscriberAddress));
            await this._subscriptionSemaphore.WaitAsync();
            try
            {
                if (_registeredSubscriptions.Contains(fullyQualifiedTopic))
                {
                    await _connectionMultiplexer.GetSubscriber().UnsubscribeAsync(fullyQualifiedTopic);
                    _registeredSubscriptions.Remove(fullyQualifiedTopic);
                }
            }
            finally
            {
                this._subscriptionSemaphore.Release();
            }
        }

        public bool IsCentralized => true;
    }
}
