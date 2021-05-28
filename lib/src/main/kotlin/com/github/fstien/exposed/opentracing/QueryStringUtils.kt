package com.github.fstien.exposed.opentracing

internal fun String.sanitize(replace: List<String>): String {
    var returnString = this

    for (str in replace) {
        returnString = returnString.replace(oldValue = str, newValue = "<REDACTED>")
    }

    return returnString
}

internal fun String.format(): String {
    return QueryStringFormatter.format(this)
}
