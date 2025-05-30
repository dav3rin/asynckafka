import logging

import asyncio

from asynckafka.exceptions import KafkaError
from asynckafka.includes cimport c_rd_kafka as crdk


logger = logging.getLogger('asynckafka')


_error_callback = None
_name_to_callback_error = {}
_name_to_callback_rebalance = {}


cdef void cb_logger(const crdk.rd_kafka_t *rk, int level, const char *fac,
                    const char *buf) noexcept:
    fac_str = bytes(fac).decode()
    buf_str = bytes(buf).decode()
    if level in {1, 2}:
        logger.critical(f"{fac_str}:{buf_str}")
    elif level == 3:
        logger.error(f"{fac_str}:{buf_str}")
    elif level in {4, 5}:
        logger.info(f"{fac_str}:{buf_str}")
    elif level in {6, 7}:
        logger.debug(f"{fac_str}:{buf_str}")


cdef void cb_error(crdk.rd_kafka_t *rk, int err, const char *reason,
                   void *opaque) noexcept:
    err_enum = <crdk.rd_kafka_resp_err_t> err
    rd_name_str = bytes(crdk.rd_kafka_name(rk)).decode()
    error_str = bytes(crdk.rd_kafka_err2str(err_enum)).decode()
    reason_str = bytes(reason).decode()
    logger.error(f"Error callback. {rd_name_str}, {error_str}, {reason_str}")

    try:
        consumer_or_producer = _name_to_callback_error[rd_name_str]
    except KeyError:
        logger.error("Error callback of not registered producer or consumer")
    else:
        kafka_error = KafkaError(rk_name=rd_name_str, error_code=err,
                                 error_str=error_str, reason=reason,
                                 consumer_or_producer=consumer_or_producer)
        try:
            asyncio.run_coroutine_threadsafe(
                consumer_or_producer.error_callback(kafka_error),
                loop=consumer_or_producer.loop
            )
        except Exception:
            logger.exception("Unexpected exception opening a error"
                             " callback corutine.")


def register_error_callback(consumer_or_producer, name):
    """
    Internal method used by the consumer and producer to register themselves.
    Args:
        consumer_or_producer (Union[asynckafka.Producer,
            asynckafka.Consumer]):
        name (str):
    """
    logger.info(f"Registering error callback of {name}")
    _name_to_callback_error[name] = consumer_or_producer


def unregister_error_callback(name):
    """
    Internal method used by the consumer and producer to unregister themselves.
    Args:
        name (str):
    """
    logger.info(f"Unregistering error callback of {name}")
    del _name_to_callback_error[name]


def register_rebalance_callback(consumer, name):
    """
    Internal method used by the consumer to register rebalance callbacks.
    Args:
        consumer (asynckafka.Consumer): Consumer instance
        name (str): Consumer name
    """
    logger.info(f"Registering rebalance callback of {name}")
    _name_to_callback_rebalance[name] = consumer


def unregister_rebalance_callback(name):
    """
    Internal method used by the consumer to unregister rebalance callbacks.
    Args:
        name (str): Consumer name
    """
    logger.info(f"Unregistering rebalance callback of {name}")
    del _name_to_callback_rebalance[name]


cdef inline log_partition_list(
        crdk.rd_kafka_topic_partition_list_t *partitions):
    string = ""
    for i in range(partitions.cnt):
        topic = partitions.elems[i].topic
        partition = partitions.elems[i].partition
        offset = partitions.elems[i].offset
        string += f"partitions=(topic={str(topic)}, " \
                  f"partition={str(partition)}, " \
                  f"offset={str(offset)})"
    logger.info(string)


cdef void cb_rebalance(crdk.rd_kafka_t *rk, crdk.rd_kafka_resp_err_t err,
        crdk.rd_kafka_topic_partition_list_t *partitions, void *opaque) noexcept:
    logger.info("consumer group rebalance")
    rd_name_str = bytes(crdk.rd_kafka_name(rk)).decode()
    
    # Get partition info for callback
    partition_info = []
    for i in range(partitions.cnt):
        topic = bytes(partitions.elems[i].topic).decode()
        partition = partitions.elems[i].partition
        offset = partitions.elems[i].offset
        partition_info.append({
            'topic': topic,
            'partition': partition,
            'offset': offset
        })
    
    if err == crdk.RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
        log_partition_list(partitions)
        rebalance_protocol = bytes(crdk.rd_kafka_rebalance_protocol(rk)).decode()
        logger.debug(f"rebalance_protocol={rebalance_protocol}")

        if rebalance_protocol == "COOPERATIVE":
            logger.debug("Using COOPERATIVE protocol")
            crdk.rd_kafka_incremental_assign(rk, partitions)
        else:
            logger.debug("Using EAGER protocol")
            crdk.rd_kafka_assign(rk, partitions)

        # Notify callback of partition assignment
        try:
            consumer = _name_to_callback_rebalance[rd_name_str]
            asyncio.run_coroutine_threadsafe(
                consumer.rebalance_callback('assign', partition_info),
                loop=consumer.loop
            )
        except KeyError:
            logger.error("Rebalance callback of not registered consumer")
        except Exception:
            logger.exception("Unexpected exception in rebalance callback")

    elif err == crdk.RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
        logger.info("partitions revoked")
        log_partition_list(partitions)

        rebalance_protocol = str(crdk.rd_kafka_rebalance_protocol(rk))
        logger.debug(f"rebalance_protocol={rebalance_protocol}")

        # Notify callback of partition revocation before actually revoking
        try:
            consumer = _name_to_callback_rebalance[rd_name_str]
            asyncio.run_coroutine_threadsafe(
                consumer.rebalance_callback('revoke', partition_info),
                loop=consumer.loop
            )
        except KeyError:
            logger.error("Rebalance callback of not registered consumer")
        except Exception:
            logger.exception("Unexpected exception in rebalance callback")

        if rebalance_protocol == "COOPERATIVE":
            logger.debug("Using COOPERATIVE protocol")
            crdk.rd_kafka_incremental_unassign(rk, partitions)
        else:
            logger.debug("Using EAGER protocol")
            crdk.rd_kafka_assign(rk, NULL)
    else:
        err_str = crdk.rd_kafka_err2str(err)
        logger.error(
            f"Error in rebalance callback, Revoked partitions {err_str}"
        )
        crdk.rd_kafka_assign(rk, NULL)
