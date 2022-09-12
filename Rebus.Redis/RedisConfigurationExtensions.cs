using System;
using System.Collections.Generic;
using Rebus.Config;
using Rebus.Injection;
using Rebus.Logging;
using Rebus.Subscriptions;
using Rebus.Transport;
using StackExchange.Redis;

namespace Rebus.Redis
{
    public static class RedisConfigurationExtensions
    {
        private const string RedisSubText = "The Redis transport was inserted as the subscriptions storage because it has native support for pub/sub messaging";

        /// <summary>
        /// Configures Rebus to use Redis to move messages around
        /// </summary>
        public static RedisOptionsBuilder UseRedis(
            this StandardConfigurer<ITransport> configurer,
            string connectionString, 
            string sectionKey,
            string inputQueueName)
        {
            if (connectionString == null)
                throw new ArgumentNullException(nameof (connectionString));
            if (inputQueueName == null)
                throw new ArgumentNullException(nameof (inputQueueName));
            return BuildInternal(configurer, false, (context, options) => new RedisTransport(connectionString,  inputQueueName, context.Get<IRebusLoggerFactory>()));
        }

        /// <summary>
        /// Configures Rebus to use Redis to move messages around
        /// </summary>
        public static RedisOptionsBuilder UseRedis(
            this StandardConfigurer<ITransport> configurer,
            ConfigurationOptions connectionOptions, 
            string sectionKey,
            string inputQueueName)
        {
            if (connectionOptions == null)
                throw new ArgumentNullException(nameof (connectionOptions));
            if (inputQueueName == null)
                throw new ArgumentNullException(nameof (inputQueueName));
            return BuildInternal(configurer, false, (Func<IResolutionContext, RedisOptionsBuilder, RedisTransport>) ((context, options) => new RedisTransport(connectionOptions,  inputQueueName, context.Get<IRebusLoggerFactory>())));
        }

        private static RedisOptionsBuilder BuildInternal(
            StandardConfigurer<ITransport> configurer,
            bool oneWay,
            Func<IResolutionContext, RedisOptionsBuilder, RedisTransport> redisTransportBuilder)
        {
            if (configurer == null)
                throw new ArgumentNullException(nameof (configurer));
            var options = new RedisOptionsBuilder();
            configurer.OtherService<RedisTransport>().Register(c =>
            {
                var transport = redisTransportBuilder(c, options);
                options.Configure(transport);
                return transport;
            });
            configurer.OtherService<ISubscriptionStorage>().Register(c => c.Get<RedisTransport>(), RedisSubText);
            configurer.Register(c => c.Get<RedisTransport>());
            if (oneWay)
                OneWayClientBackdoor.ConfigureOneWayClient(configurer);
            return options;
        }
    }

    public class RedisOptionsBuilder
    {
        public void Configure(RedisTransport transport)
        {
            
        }
    }
}