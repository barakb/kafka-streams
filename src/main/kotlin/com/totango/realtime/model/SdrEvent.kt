package com.totango.realtime.model
data class SdrEvent(
    val timestamp: String,
    val serviceId: String? = null,
    val accountId: String? = null,
    val moduleId: String? = null,
    val activity: String? = null,
    val user: String? = null
)
