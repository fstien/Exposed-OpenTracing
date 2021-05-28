package com.github.fstien.exposed.opentracing

import java.util.HashSet
import java.util.LinkedList
import java.lang.StringBuilder
import java.util.StringTokenizer
import java.util.Locale

internal class QueryStringFormatter {
    companion object {
        private val BEGIN_CLAUSES: MutableSet<String> = HashSet()
        private val END_CLAUSES: MutableSet<String> = HashSet()
        private val LOGICAL: MutableSet<String> = HashSet()
        private val QUANTIFIERS: MutableSet<String> = HashSet()
        private val DML: MutableSet<String> = HashSet()
        private val MISC: MutableSet<String> = HashSet()
        private const val WHITESPACE = " \n\r\t"
        private const val INDENT_STRING = "    "

        init {
            BEGIN_CLAUSES.add("left")
            BEGIN_CLAUSES.add("right")
            BEGIN_CLAUSES.add("inner")
            BEGIN_CLAUSES.add("outer")
            BEGIN_CLAUSES.add("group")
            BEGIN_CLAUSES.add("order")
            END_CLAUSES.add("where")
            END_CLAUSES.add("set")
            END_CLAUSES.add("having")
            END_CLAUSES.add("from")
            END_CLAUSES.add("by")
            END_CLAUSES.add("join")
            END_CLAUSES.add("into")
            END_CLAUSES.add("union")
            LOGICAL.add("and")
            LOGICAL.add("or")
            LOGICAL.add("when")
            LOGICAL.add("else")
            LOGICAL.add("end")
            QUANTIFIERS.add("in")
            QUANTIFIERS.add("all")
            QUANTIFIERS.add("exists")
            QUANTIFIERS.add("some")
            QUANTIFIERS.add("any")
            DML.add("insert")
            DML.add("update")
            DML.add("delete")
            MISC.add("select")
            MISC.add("on")
        }

        fun format(source: String): String {
            return FormatProcess(source).perform()
        }
    }

    private class FormatProcess(sql: String) {
        var beginLine = true
        var afterBeginBeforeEnd = false
        var afterByOrSetOrFromOrSelect = false
        var afterOn = false
        var afterBetween = false
        var afterInsert = false
        var inFunction = 0
        var parensSinceSelect = 0
        private val parenCounts = LinkedList<Int>()
        private val afterByOrFromOrSelects = LinkedList<Boolean>()
        var indent = 1
        var result = StringBuilder()
        var tokens: StringTokenizer
        var lastToken: String? = null
        var token: String? = null
        var lcToken: String? = null
        fun perform(): String {
            while (tokens.hasMoreTokens()) {
                token = tokens.nextToken()
                lcToken = token?.toLowerCase(Locale.ROOT)
                if ("'" == token) {
                    var t: String
                    do {
                        t = tokens.nextToken()
                        token += t
                    } while ("'" != t && tokens.hasMoreTokens())
                } else if ("\"" == token) {
                    var t: String
                    do {
                        t = tokens.nextToken()
                        token += t
                    } while ("\"" != t && tokens.hasMoreTokens())
                } else if ("[" == token) {
                    var t: String
                    do {
                        t = tokens.nextToken()
                        token += t
                    } while ("]" != t && tokens.hasMoreTokens())
                }
                if (afterByOrSetOrFromOrSelect && "," == token) {
                    commaAfterByOrFromOrSelect()
                } else if (afterOn && "," == token) {
                    commaAfterOn()
                } else if ("(" == token) {
                    openParen()
                } else if (")" == token) {
                    closeParen()
                } else if (BEGIN_CLAUSES.contains(lcToken)) {
                    beginNewClause()
                } else if (END_CLAUSES.contains(lcToken)) {
                    endNewClause()
                } else if ("select" == lcToken) {
                    select()
                } else if (DML.contains(lcToken)) {
                    updateOrInsertOrDelete()
                } else if ("values" == lcToken) {
                    values()
                } else if ("on" == lcToken) {
                    on()
                } else if (afterBetween && lcToken == "and") {
                    misc()
                    afterBetween = false
                } else if (LOGICAL.contains(lcToken)) {
                    logical()
                } else if (isWhitespace(token)) {
                    white()
                } else {
                    misc()
                }
                if (!isWhitespace(token)) {
                    lastToken = lcToken
                }
            }
            return result.toString()
        }

        private fun commaAfterOn() {
            out()
            indent--
            newline()
            afterOn = false
            afterByOrSetOrFromOrSelect = true
        }

        private fun commaAfterByOrFromOrSelect() {
            out()
            newline()
        }

        private fun logical() {
            if ("end" == lcToken) {
                indent--
            }
            newline()
            out()
            beginLine = false
        }

        private fun on() {
            indent++
            afterOn = true
            newline()
            out()
            beginLine = false
        }

        private fun misc() {
            out()
            if ("between" == lcToken) {
                afterBetween = true
            }
            if (afterInsert) {
                newline()
                afterInsert = false
            } else {
                beginLine = false
                if ("case" == lcToken) {
                    indent++
                }
            }
        }

        private fun white() {
            if (!beginLine) {
                result.append(" ")
            }
        }

        private fun updateOrInsertOrDelete() {
            out()
            indent++
            beginLine = false
            if ("update" == lcToken) {
                newline()
            }
            if ("insert" == lcToken) {
                afterInsert = true
            }
        }

        private fun select() {
            out()
            indent++
            newline()
            parenCounts.addLast(parensSinceSelect)
            afterByOrFromOrSelects.addLast(afterByOrSetOrFromOrSelect)
            parensSinceSelect = 0
            afterByOrSetOrFromOrSelect = true
        }

        private fun out() {
            result.append(token)
        }

        private fun endNewClause() {
            if (!afterBeginBeforeEnd) {
                indent--
                if (afterOn) {
                    indent--
                    afterOn = false
                }
                newline()
            }
            out()
            if ("union" != lcToken) {
                indent++
            }
            newline()
            afterBeginBeforeEnd = false
            afterByOrSetOrFromOrSelect = "by" == lcToken || "set" == lcToken || "from" == lcToken
        }

        private fun beginNewClause() {
            if (!afterBeginBeforeEnd) {
                if (afterOn) {
                    indent--
                    afterOn = false
                }
                indent--
                newline()
            }
            out()
            beginLine = false
            afterBeginBeforeEnd = true
        }

        private fun values() {
            indent--
            newline()
            out()
            indent++
            newline()
        }

        private fun closeParen() {
            parensSinceSelect--
            if (parensSinceSelect < 0) {
                indent--
                parensSinceSelect = parenCounts.removeLast()
                afterByOrSetOrFromOrSelect = afterByOrFromOrSelects.removeLast()
            }
            if (inFunction > 0) {
                inFunction--
                out()
            } else {
                if (!afterByOrSetOrFromOrSelect) {
                    indent--
                    newline()
                }
                out()
            }
            beginLine = false
        }

        private fun openParen() {
            if (isFunctionName(lastToken) || inFunction > 0) {
                inFunction++
            }
            beginLine = false
            if (inFunction > 0) {
                out()
            } else {
                out()
                if (!afterByOrSetOrFromOrSelect) {
                    indent++
                    newline()
                    beginLine = true
                }
            }
            parensSinceSelect++
        }

        private fun newline() {
            result.append(System.lineSeparator())
            for (i in 0 until indent) {
                result.append(INDENT_STRING)
            }
            beginLine = true
        }

        companion object {
            private fun isFunctionName(tok: String?): Boolean {
                if (tok == null || tok.length == 0) {
                    return false
                }
                val begin = tok[0]
                val isIdentifier = Character.isJavaIdentifierStart(begin) || '"' == begin
                return isIdentifier &&
                        !LOGICAL.contains(tok) &&
                        !END_CLAUSES.contains(tok) &&
                        !QUANTIFIERS.contains(tok) &&
                        !DML.contains(tok) &&
                        !MISC.contains(tok)
            }

            private fun isWhitespace(token: String?): Boolean {
                return WHITESPACE.contains(token!!)
            }
        }

        init {
            tokens = StringTokenizer(
                sql,
                "()+*/-=<>'`\"[]," + WHITESPACE,
                true
            )
        }
    }
}