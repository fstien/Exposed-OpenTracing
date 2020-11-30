package com.github.fstien.exposed.opentracing

import mu.KotlinLogging


private val log = KotlinLogging.logger {}

internal fun validInputs(replacePii: List<String>, contains: Contains): Boolean {
    when (contains) {
        Contains.PII -> {
            if (replacePii.isEmpty()) {
                log.warn("replace varargs argument should not be empty if transaction contains PII")
                return false
            }
        }
        Contains.NoPII -> {
            if (replacePii.isNotEmpty()) {
                log.warn("replace varargs argument should be empty if transaction contains no PII")
                return false
            }
        }
    }

    return true
}