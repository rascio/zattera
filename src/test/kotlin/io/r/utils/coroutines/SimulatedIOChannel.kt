package io.r.utils.coroutines

import kotlinx.coroutines.channels.Channel

class SimulatedIOChannel<E>(private val channel: Channel<E>) : Channel<E> by channel {


}