import asyncio
import logging
from typing import List, Optional, Callable, Coroutine, Any, Dict

from asynckafka import exceptions, utils
from asynckafka.callbacks import register_error_callback, \
    unregister_error_callback, register_rebalance_callback, unregister_rebalance_callback
from asynckafka.consumer.message cimport message_factory
from asynckafka.consumer.message cimport message_factory_no_destroy
from asynckafka.consumer.message cimport message_destroy
from asynckafka.consumer.rd_kafka_consumer cimport RdKafkaConsumer
from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka.settings cimport debug
from asynckafka.exceptions import KafkaError


logger = logging.getLogger('asynckafka')


cdef class Consumer:
    """
    An async Kafka consumer that can be used as an async iterator.
    
    Example:
        Consumer works as an async iterator::

            consumer = Consumer(['my_topic'], brokers='localhost:9092')
            consumer.start()

            async for message in consumer:
                print(message.payload)
                
            # When done
            consumer.stop()

    Args:
        topics (List[str]): List of topics to consume from
        brokers (Optional[str]): Comma-separated list of brokers, e.g. "host1:9092,host2:9092"
        group_id (Optional[str]): Consumer group identifier
        rdk_consumer_config (Optional[dict]): Additional rdkafka consumer configuration
        rdk_topic_config (Optional[dict]): Additional rdkafka topic configuration  
        error_callback (Optional[Callable[[KafkaError], Coroutine[Any, Any, None]]]): 
            Async callback for error handling. Receives a KafkaError object.
        rebalance_callback (Optional[Callable[[str, List[dict]], Coroutine[Any, Any, None]]]): 
            Async callback for rebalance events. Receives event_type ('assign' or 'revoke') and 
            list of partition info dicts with keys: topic (str), partition (int), offset (int)
        loop (Optional[asyncio.AbstractEventLoop]): Event loop to use
        poll_interval (float): Sleep interval between polls when no messages (default: 0.025)

    Raises:
        exceptions.ConsumerError: On consumer initialization errors
        exceptions.InvalidSetting: On invalid configuration
        exceptions.UnknownSetting: On unknown configuration options
    """
    def __init__(self, topics: List[str], brokers: Optional[str] = None, group_id: Optional[str] = None,
                 rdk_consumer_config: Optional[dict] = None, rdk_topic_config: Optional[dict] = None,
                 error_callback: Optional[Callable[[KafkaError], Coroutine[Any, Any, None]]] = None,
                 rebalance_callback: Optional[Callable[[str, List[dict]], Coroutine[Any, Any, None]]] = None,
                 loop: Optional[asyncio.AbstractEventLoop] = None, poll_interval: float = 0.025):
        # TODO add auto_partition_assigment as parameter
        self.rdk_consumer = RdKafkaConsumer(
            brokers=brokers, group_id=group_id, topic_config=rdk_topic_config,
            consumer_config=rdk_consumer_config
        )
        assert isinstance(topics, list), "Topics should be a list"
        assert len(topics) >= 1, "It is necessary at least one topic"
        self.topics = [topic.encode() for topic in topics]
        [self.rdk_consumer.add_topic(topic) for topic in topics]
        self.loop = loop if loop else asyncio.get_event_loop()
        self.consumer_state = consumer_states.NOT_CONSUMING
        self.poll_rd_kafka_task = None
        self.error_callback = error_callback
        self.rebalance_callback = rebalance_callback
        self.poll_interval = poll_interval


    def seek(self, topic_partition, timeout: int = 500) -> None:
        """
        Seek consumer to a specific offset for a topic partition.

        Args:
            topic_partition (TopicPartition): Topic partition to seek
            timeout (int): Operation timeout in milliseconds

        Raises:
            exceptions.ConsumerError: If seek operation times out
        """
        self.rdk_consumer.seek(topic_partition, timeout)

    def is_consuming(self):
        """
        Method for check the consumer state.

        Returns:
            bool: True if the consumer is consuming false if not.
        """
        return self.consumer_state == consumer_states.CONSUMING

    def is_stopped(self):
        """
        Method for check the consumer state.

        Returns:
            bool: True if the consumer is stopped false if not.
        """
        return not self.is_consuming()

    def start(self):
        """
        Start the consumer. It is necessary call this method before start to
        consume messages.

        Raises:
            asynckafka.exceptions.ConsumerError: Error in the initialization of
                the consumer client.
            asynckafka.exceptions.InvalidSetting: Invalid setting in
                consumer_settings or topic_settings.
            asynckafka.exceptions.UnknownSetting: Unknown setting in
                consumer_settings or topic_settings.
        """
        if self.is_consuming():
            logger.error("Tried to start a consumer that it is already "
                         "running")
            raise exceptions.ConsumerError("Consumer is already running")
        else:
            logger.info("Called consumer start")
            self.rdk_consumer.start()
            logger.info("Creating rd kafka poll task")
            self.poll_rd_kafka_task = asyncio.ensure_future(
                utils.periodic_rd_kafka_poll(
                    <long> self.rdk_consumer.consumer,
                    self.loop
                ), loop=self.loop
            )
            self._post_start()
            if self.error_callback:
                register_error_callback(
                    self, self.rdk_consumer.get_name())
            if self.rebalance_callback:
                register_rebalance_callback(
                    self, self.rdk_consumer.get_name())
            self.consumer_state = consumer_states.CONSUMING
            logger.info('Consumer started')

    def assign_topic_offset(self, topic: str, partition: int, offset: int) -> None:
        """
        Manually assign consumer to specific topic partition and offset.

        Args:
            topic (str): Topic name
            partition (int): Partition number
            offset (int): Offset to start consuming from
        """
        self.rdk_consumer.assign_topic_offset(topic, partition, offset)

    def _post_start(self):
        """
        Internal method to be overwritten by other consumers that use this one
        as Base.
        """
        pass

    def stop(self):
        """
        Stop the consumer. It is advisable to call this method before
        closing the python interpreter. It are going to stop the
        asynciterator, asyncio tasks opened by this client and free the
        memory used by the consumer.

        Raises:
            asynckafka.exceptions.ConsumerError: Error in the shut down of
                the consumer client.
            asynckafka.exceptions.InvalidSetting: Invalid setting in
                consumer_settings or topic_settings.
            asynckafka.exceptions.UnknownSetting: Unknown setting in
                consumer_settings or topic_settings.
        """
        if not self.is_consuming():
            logger.error("Tried to stop a consumer that it is already "
                         "stopped")
            raise exceptions.ConsumerError("Consumer isn't running")
        else:
            logger.info("Stopping asynckafka consumer")
            self._pre_stop()
            if self.error_callback:
                unregister_error_callback(self.rdk_consumer.get_name())
            if self.rebalance_callback:
                unregister_rebalance_callback(self.rdk_consumer.get_name())
            logger.info("Closing rd kafka poll task")
            self.poll_rd_kafka_task.cancel()
            self.rdk_consumer.stop()
            logger.info("Stopped correctly asynckafka consumer")
            self.consumer_state = consumer_states.NOT_CONSUMING

    def _pre_stop(self):
        """
        Internal method to be overwritten by other consumers that use this one
        as Base.
        """
        pass

    def assignment(self):
        """
        Partition assignment of the consumer.

        Returns:
            [asynckafka.consumer.topic_partition.TopicPartition]: List with
                the current partition assignment of the consumer.
        """
        return self.rdk_consumer.assignment()

    async def commit_partitions(self, partitions):
        """
        Commit topic partitions.

        Args:
            [asynckafka.consumer.topic_partition.TopicPartition]: topic
                partitions to commit.
        """
        pass

    async def offset_store(self):
        pass

    def __aiter__(self):
        """
        Allows the consumer to work as a async iterator.
        """
        if self.is_consuming():
            return self
        else:
            raise exceptions.ConsumerError("Consumer isn't consuming")

    async def next_commited(self):
        """
        Polls rdkafka consumer and build the message object.

        Returns:
            asynckafka.consumer.message.Message: Message consumed.
        """
        cdef crdk.rd_kafka_message_t *rk_message
        try:
            while self.consumer_state == consumer_states.CONSUMING:
                    rk_message = crdk.rd_kafka_consumer_poll(
                        self.rdk_consumer.consumer, 0)
                    if rk_message:
                        message = message_factory_no_destroy(rk_message)
                        if not message.error:
                            yield message
                        else:
                            raise exceptions.ConsumerError("Message error")
                        
                        crdk.rd_kafka_commit_message(
                            self.rdk_consumer.consumer, rk_message, 0
                        )
                        message_destroy(rk_message)
                    else:
                        if debug: logger.debug("Poll timeout without messages")
                        await asyncio.sleep(
                            self.poll_interval
                        )
        except asyncio.CancelledError:
            logger.info("Poll consumer thread task canceled")
            raise
        except Exception:
            logger.error(
                "Unexpected exception consuming messages from thread",
                exc_info=True
            )
        raise StopAsyncIteration


    async def __anext__(self):
        """
        Polls rdkafka consumer and build the message object.

        Returns:
            asynckafka.consumer.message.Message: Message consumed.
        """
        cdef crdk.rd_kafka_message_t *rk_message
        try:
            while self.consumer_state == consumer_states.CONSUMING:
                    rk_message = crdk.rd_kafka_consumer_poll(
                        self.rdk_consumer.consumer, 0)
                    if rk_message:
                        message = message_factory(rk_message)
                        if not message.error:
                            return message
                        else:
                            raise exceptions.ConsumerError("Message error")
                    else:
                        if debug: logger.debug("Poll timeout without messages")
                        await asyncio.sleep(
                            self.poll_interval
                        )
        except asyncio.CancelledError:
            logger.info("Poll consumer thread task canceled")
            raise
        except Exception:
            logger.error(
                "Unexpected exception consuming messages from thread",
                exc_info=True
            )
        raise StopAsyncIteration