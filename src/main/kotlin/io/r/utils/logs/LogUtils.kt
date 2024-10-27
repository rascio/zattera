package io.r.utils.logs

import org.apache.logging.log4j.message.StringMapMessage

/**
 * Utility function to generate a structured log entry.
 * Used as:
 * ```
 * logger.info { entry("event_name", "key1" to "value1", "key2" to "value2") }
 * logger.warn(
 *  entry("event_name", "key1" to "value1", "key2" to "value2"),
 *  throwable
 * )
 * ```
 * @param name the entry name, prefer a short, snake_case string
 * @param data the key-value pairs to be logged
 */
fun entry(name: String, vararg data: Pair<String, Any?>): StringMapMessage {
    return StringMapMessage(data.size + 1).apply {
        data.forEach { (k, v) ->
            put(k, v.toString())
        }
        put("_event", name)
    }
}
