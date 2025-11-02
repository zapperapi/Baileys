import { EventEmitter } from 'events'
import { URL } from 'url'
import type { SocketConfig } from '../../Types'

export abstract class AbstractSocketClient extends EventEmitter {
	abstract get isOpen(): boolean
	abstract get isClosed(): boolean
	abstract get isClosing(): boolean
	abstract get isConnecting(): boolean

	constructor(
		public url: URL,
		public config: SocketConfig
	) {
		super()
		// Set max listeners to detect memory leaks from accumulating event listeners
		// 50 is a reasonable limit for the abstract socket client
		this.setMaxListeners(50)
	}

	abstract connect(): Promise<void>
	abstract close(): Promise<void>
	abstract send(str: Uint8Array | string, cb?: (err?: Error) => void): boolean
}
