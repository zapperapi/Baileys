import EventEmitter from 'events'
import { LRUCache } from 'lru-cache'
import type {
	BaileysEvent,
	BaileysEventEmitter,
	BaileysEventMap,
	BufferedEventData,
	Chat,
	ChatUpdate,
	Contact,
	WAMessage,
	WAMessageKey
} from '../Types'
import { WAMessageStatus } from '../Types'
import { trimUndefined } from './generics'
import type { ILogger } from './logger'
import { updateMessageWithReaction, updateMessageWithReceipt } from './messages'
import { isRealMessage, shouldIncrementChatUnread } from './process-message'

const BUFFERABLE_EVENT = [
	'messaging-history.set',
	'chats.upsert',
	'chats.update',
	'chats.delete',
	'contacts.upsert',
	'contacts.update',
	'messages.upsert',
	'messages.update',
	'messages.delete',
	'messages.reaction',
	'message-receipt.update',
	'groups.update'
] as const

type BufferableEvent = (typeof BUFFERABLE_EVENT)[number]

// Constants for conditional update management
const CONDITIONAL_UPDATE_TIMEOUT_MS = 5 * 60 * 1000 // 5 minutes for conditional updates
const MAX_CONDITIONAL_UPDATES = 100 // Maximum number of pending conditional updates

/**
 * A map that contains a list of all events that have been triggered
 *
 * Note, this can contain different type of events
 * this can make processing events extremely efficient -- since everything
 * can be done in a single transaction
 */
type BaileysEventData = Partial<BaileysEventMap>

const BUFFERABLE_EVENT_SET = new Set<BaileysEvent>(BUFFERABLE_EVENT)

type BaileysBufferableEventEmitter = BaileysEventEmitter & {
	/** Use to process events in a batch */
	process(handler: (events: BaileysEventData) => void | Promise<void>): () => void
	/**
	 * starts buffering events, call flush() to release them
	 * */
	buffer(): void
	/** buffers all events till the promise completes */
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	createBufferedFunction<A extends any[], T>(work: (...args: A) => Promise<T>): (...args: A) => Promise<T>
	/**
	 * flushes all buffered events
	 * @returns returns true if the flush actually happened, otherwise false
	 */
	flush(): boolean
	/** is there an ongoing buffer */
	isBuffering(): boolean
}

/**
 * The event buffer logically consolidates different events into a single event
 * making the data processing more efficient.
 */
export const makeEventBuffer = (logger: ILogger): BaileysBufferableEventEmitter => {
	const ev = new EventEmitter()
	// Set max listeners to detect memory leaks from accumulating event listeners
	// 50 is a reasonable limit - if exceeded, Node.js will emit a warning
	ev.setMaxListeners(50)

	// Use LRU cache instead of Set to automatically evict least recently used items
	const historyCache = new LRUCache<string, boolean>({
		max: 5000, // Reduced from 10000 to use LRU eviction more frequently
		ttl: 30 * 60 * 1000, // 30 minutes TTL for cache entries
		updateAgeOnGet: false
	})

	let data = makeBufferData()
	let isBuffering = false
	let bufferTimeout: NodeJS.Timeout | null = null
	let conditionalCleanupTimer: NodeJS.Timeout | null = null
	let bufferCount = 0
	const activeBufferedTimers = new Set<NodeJS.Timeout>() // Track all active timers from createBufferedFunction
	const BUFFER_TIMEOUT_MS = 30000 // 30 seconds
	const CONDITIONAL_CLEANUP_INTERVAL_MS = 60 * 1000 // Cleanup expired conditionals every minute
	const MAX_BUFFER_SIZE_MESSAGES = 10000 // Maximum number of messages before forcing flush
	const MAX_BUFFER_SIZE_CHATS = 5000 // Maximum number of chats before forcing flush
	const MAX_BUFFER_SIZE_CONTACTS = 5000 // Maximum number of contacts before forcing flush

	// take the generic event and fire it as a baileys event
	ev.on('event', (map: BaileysEventData) => {
		for (const event in map) {
			ev.emit(event, map[event as keyof BaileysEventMap])
		}
	})

	// Periodic cleanup of expired conditional updates
	const startConditionalCleanup = () => {
		if (conditionalCleanupTimer) {
			clearInterval(conditionalCleanupTimer)
		}

		conditionalCleanupTimer = setInterval(() => {
			cleanupExpiredConditionalUpdates()
		}, CONDITIONAL_CLEANUP_INTERVAL_MS)
	}

	const cleanupExpiredConditionalUpdates = () => {
		const now = Date.now()
		let expiredCount = 0
		const chatUpdates = Object.keys(data.chatUpdates)

		for (const chatId of chatUpdates) {
			const update = data.chatUpdates[chatId]
			if (update?.conditional && update.timestamp) {
				const isExpired = now - update.timestamp > CONDITIONAL_UPDATE_TIMEOUT_MS
				if (isExpired) {
					logger.debug({ chatId }, 'Cleaning up expired conditional chat update during periodic cleanup')
					delete data.chatUpdates[chatId]
					expiredCount++
				}
			}
		}

		if (expiredCount > 0) {
			logger.debug({ expiredCount }, 'Cleaned up expired conditional updates')
		}
	}

	// Start periodic cleanup
	startConditionalCleanup()

	// Calculate buffer size
	const getBufferSize = (): { messages: number; chats: number; contacts: number; total: number } => {
		const messages =
			Object.keys(data.historySets.messages).length +
			Object.keys(data.messageUpserts).length +
			Object.keys(data.messageUpdates).length +
			Object.keys(data.messageDeletes).length +
			Object.keys(data.messageReactions).length +
			Object.keys(data.messageReceipts).length

		const chats =
			Object.keys(data.historySets.chats).length +
			Object.keys(data.chatUpserts).length +
			Object.keys(data.chatUpdates).length +
			data.chatDeletes.size

		const contacts =
			Object.keys(data.historySets.contacts).length +
			Object.keys(data.contactUpserts).length +
			Object.keys(data.contactUpdates).length

		return {
			messages,
			chats,
			contacts,
			total: messages + chats + contacts
		}
	}

	// Check if buffer size exceeds limits and force flush if necessary
	const checkBufferSize = () => {
		const size = getBufferSize()
		let shouldFlush = false
		const reasons: string[] = []

		if (size.messages >= MAX_BUFFER_SIZE_MESSAGES) {
			shouldFlush = true
			reasons.push(`messages (${size.messages} >= ${MAX_BUFFER_SIZE_MESSAGES})`)
		}

		if (size.chats >= MAX_BUFFER_SIZE_CHATS) {
			shouldFlush = true
			reasons.push(`chats (${size.chats} >= ${MAX_BUFFER_SIZE_CHATS})`)
		}

		if (size.contacts >= MAX_BUFFER_SIZE_CONTACTS) {
			shouldFlush = true
			reasons.push(`contacts (${size.contacts} >= ${MAX_BUFFER_SIZE_CONTACTS})`)
		}

		if (shouldFlush) {
			logger.warn({ bufferSize: size, reasons: reasons.join(', ') }, 'Buffer size limit exceeded, forcing flush')
			flush()
		} else if (size.total > 0 && size.total % 1000 === 0) {
			// Log progress every 1000 items to help monitor buffer growth
			logger.debug({ bufferSize: size }, 'Buffer size check')
		}
	}

	// Clear all active timers from createBufferedFunction
	const clearAllBufferedTimers = () => {
		if (activeBufferedTimers.size > 0) {
			logger.debug({ timerCount: activeBufferedTimers.size }, 'Clearing active buffered function timers')
			for (const timer of activeBufferedTimers) {
				clearTimeout(timer)
			}

			activeBufferedTimers.clear()
		}
	}

	function buffer() {
		if (!isBuffering) {
			logger.debug('Event buffer activated')
			isBuffering = true
			bufferCount++

			// Auto-flush after a timeout to prevent infinite buffering
			if (bufferTimeout) {
				clearTimeout(bufferTimeout)
			}

			// Clear any existing buffered function timers when starting new buffer
			clearAllBufferedTimers()

			bufferTimeout = setTimeout(() => {
				if (isBuffering) {
					logger.warn('Buffer timeout reached, auto-flushing')
					flush()
				}
			}, BUFFER_TIMEOUT_MS)
		} else {
			bufferCount++
		}
	}

	function flush() {
		if (!isBuffering) {
			return false
		}

		logger.debug({ bufferCount }, 'Flushing event buffer')
		isBuffering = false
		bufferCount = 0

		// Clear timeout
		if (bufferTimeout) {
			clearTimeout(bufferTimeout)
			bufferTimeout = null
		}

		// Clear all active buffered function timers
		clearAllBufferedTimers()

		// Cleanup expired conditionals during flush as well
		cleanupExpiredConditionalUpdates()

		const newData = makeBufferData()
		const chatUpdates = Object.values(data.chatUpdates)
		let conditionalChatUpdatesLeft = 0
		const now = Date.now()

		for (const update of chatUpdates) {
			if (update.conditional) {
				// Check if conditional update has expired
				const updateTimestamp = update.timestamp || 0
				const isExpired = now - updateTimestamp > CONDITIONAL_UPDATE_TIMEOUT_MS

				if (isExpired) {
					logger.debug({ chatId: update.id }, 'Removing expired conditional chat update')
					delete data.chatUpdates[update.id!]
					continue
				}

				conditionalChatUpdatesLeft += 1
				newData.chatUpdates[update.id!] = update
				delete data.chatUpdates[update.id!]
			}
		}

		const consolidatedData = consolidateEvents(data)
		if (Object.keys(consolidatedData).length) {
			ev.emit('event', consolidatedData)
		}

		data = newData

		logger.trace({ conditionalChatUpdatesLeft }, 'released buffered events')

		return true
	}

	return {
		process(handler) {
			const listener = (map: BaileysEventData) => {
				handler(map)
			}

			ev.on('event', listener)
			return () => {
				ev.off('event', listener)
			}
		},
		emit<T extends BaileysEvent>(event: BaileysEvent, evData: BaileysEventMap[T]) {
			if (isBuffering && BUFFERABLE_EVENT_SET.has(event)) {
				append(data, historyCache, event as BufferableEvent, evData, logger)
				// Check buffer size after appending to prevent excessive growth
				checkBufferSize()
				return true
			}

			return ev.emit('event', { [event]: evData })
		},
		isBuffering() {
			return isBuffering
		},
		buffer,
		flush,
		createBufferedFunction(work) {
			return async (...args) => {
				buffer()
				try {
					const result = await work(...args)
					// If this is the only buffer, flush after a small delay
					if (bufferCount === 1) {
						const timer = setTimeout(() => {
							activeBufferedTimers.delete(timer)
							if (isBuffering && bufferCount === 1) {
								flush()
							}
						}, 100) // Small delay to allow nested buffers
						activeBufferedTimers.add(timer)
					}

					return result
				} catch (error) {
					throw error
				} finally {
					bufferCount = Math.max(0, bufferCount - 1)
					if (bufferCount === 0) {
						// Auto-flush when no other buffers are active
						const timer = setTimeout(() => {
							activeBufferedTimers.delete(timer)
							flush()
						}, 100)
						activeBufferedTimers.add(timer)
					}
				}
			}
		},
		on: (...args) => ev.on(...args),
		off: (...args) => ev.off(...args),
		removeAllListeners: (...args) => ev.removeAllListeners(...args)
	}
}

const makeBufferData = (): BufferedEventData => {
	return {
		historySets: {
			chats: {},
			messages: {},
			contacts: {},
			isLatest: false,
			empty: true
		},
		chatUpserts: {},
		chatUpdates: {},
		chatDeletes: new Set(),
		contactUpserts: {},
		contactUpdates: {},
		messageUpserts: {},
		messageUpdates: {},
		messageReactions: {},
		messageDeletes: {},
		messageReceipts: {},
		groupUpdates: {}
	}
}

// Remove oldest conditional update if limit is reached
function handleConditionalUpdateLimit(data: BufferedEventData, chatId: string, logger: ILogger) {
	const conditionalCount = Object.values(data.chatUpdates).filter(u => u?.conditional).length
	if (conditionalCount < MAX_CONDITIONAL_UPDATES) {
		return
	}

	logger.warn({ chatId, conditionalCount }, 'Maximum conditional updates reached, removing oldest pending update')

	// Find oldest conditional update
	let oldestId: string | undefined
	let oldestTimestamp = Infinity
	for (const [id, upd] of Object.entries(data.chatUpdates)) {
		if (!upd?.conditional) {
			continue
		}

		const ts = upd.timestamp || 0
		if (ts < oldestTimestamp) {
			oldestTimestamp = ts
			oldestId = id
		}
	}

	if (oldestId) {
		delete data.chatUpdates[oldestId]
		logger.debug({ removedChatId: oldestId }, 'Removed oldest conditional update to make room')
	}
}

function absorbingChatUpdate(existing: Chat, data: BufferedEventData, logger: ILogger) {
	const chatId = existing.id || ''
	const update = data.chatUpdates[chatId]
	if (update) {
		// Check if update has expired before processing
		if (update.conditional && update.timestamp) {
			const now = Date.now()
			const isExpired = now - update.timestamp > CONDITIONAL_UPDATE_TIMEOUT_MS
			if (isExpired) {
				logger.debug({ chatId }, 'Skipping expired conditional update during absorption')
				delete data.chatUpdates[chatId]
				return
			}
		}

		const conditionMatches = update.conditional ? update.conditional(data) : true
		if (conditionMatches) {
			delete update.conditional
			logger.debug({ chatId }, 'absorbed chat update in existing chat')
			Object.assign(existing, concatChats(update as Chat, existing))
			delete data.chatUpdates[chatId]
		} else if (conditionMatches === false) {
			logger.debug({ chatId }, 'chat update condition fail, removing')
			delete data.chatUpdates[chatId]
		}
	}
}

function append<E extends BufferableEvent>(
	data: BufferedEventData,
	historyCache: LRUCache<string, boolean>,
	event: E,
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	eventData: any,
	logger: ILogger
) {
	switch (event) {
		case 'messaging-history.set':
			for (const chat of eventData.chats as Chat[]) {
				const id = chat.id || ''
				const existingChat = data.historySets.chats[id]
				if (existingChat) {
					existingChat.endOfHistoryTransferType = chat.endOfHistoryTransferType
				}

				if (!existingChat && !historyCache.has(id)) {
					data.historySets.chats[id] = chat
					historyCache.set(id, true)

					absorbingChatUpdate(chat, data, logger)
				}
			}

			for (const contact of eventData.contacts as Contact[]) {
				const existingContact = data.historySets.contacts[contact.id]
				if (existingContact) {
					Object.assign(existingContact, trimUndefined(contact))
				} else {
					const historyContactId = `c:${contact.id}`
					const hasAnyName = contact.notify || contact.name || contact.verifiedName
					if (!historyCache.has(historyContactId) || hasAnyName) {
						data.historySets.contacts[contact.id] = contact
						historyCache.set(historyContactId, true)
					}
				}
			}

			for (const message of eventData.messages as WAMessage[]) {
				const key = stringifyMessageKey(message.key)
				const existingMsg = data.historySets.messages[key]
				if (!existingMsg && !historyCache.has(key)) {
					data.historySets.messages[key] = message
					historyCache.set(key, true)
				}
			}

			data.historySets.empty = false
			data.historySets.syncType = eventData.syncType
			data.historySets.progress = eventData.progress
			data.historySets.peerDataRequestSessionId = eventData.peerDataRequestSessionId
			data.historySets.isLatest = eventData.isLatest || data.historySets.isLatest

			break
		case 'chats.upsert':
			for (const chat of eventData as Chat[]) {
				const id = chat.id || ''
				let upsert = data.chatUpserts[id]
				if (id && !upsert) {
					upsert = data.historySets.chats[id]
					if (upsert) {
						logger.debug({ chatId: id }, 'absorbed chat upsert in chat set')
					}
				}

				if (upsert) {
					upsert = concatChats(upsert, chat)
				} else {
					upsert = chat
					data.chatUpserts[id] = upsert
				}

				absorbingChatUpdate(upsert, data, logger)

				if (data.chatDeletes.has(id)) {
					data.chatDeletes.delete(id)
				}
			}

			break
		case 'chats.update':
			for (const update of eventData as ChatUpdate[]) {
				const chatId = update.id!
				const conditionMatches = update.conditional ? update.conditional(data) : true
				if (conditionMatches) {
					delete update.conditional

					// if there is an existing upsert, merge the update into it
					const upsert = data.historySets.chats[chatId] || data.chatUpserts[chatId]
					if (upsert) {
						concatChats(upsert, update)
					} else {
						// merge the update into the existing update
						const chatUpdate = data.chatUpdates[chatId] || {}
						data.chatUpdates[chatId] = concatChats(chatUpdate, update)
					}
				} else if (conditionMatches === undefined) {
					// condition yet to be fulfilled
					handleConditionalUpdateLimit(data, chatId, logger)

					// Add timestamp to track expiration
					if (!update.timestamp) {
						update.timestamp = Date.now()
					}

					data.chatUpdates[chatId] = update
				}
				// otherwise -- condition not met, update is invalid

				// if the chat has been updated
				// ignore any existing chat delete
				if (data.chatDeletes.has(chatId)) {
					data.chatDeletes.delete(chatId)
				}
			}

			break
		case 'chats.delete':
			for (const chatId of eventData as string[]) {
				if (!data.chatDeletes.has(chatId)) {
					data.chatDeletes.add(chatId)
				}

				// remove any prior updates & upserts
				if (data.chatUpdates[chatId]) {
					delete data.chatUpdates[chatId]
				}

				if (data.chatUpserts[chatId]) {
					delete data.chatUpserts[chatId]
				}

				if (data.historySets.chats[chatId]) {
					delete data.historySets.chats[chatId]
				}
			}

			break
		case 'contacts.upsert':
			for (const contact of eventData as Contact[]) {
				let upsert = data.contactUpserts[contact.id]
				if (!upsert) {
					upsert = data.historySets.contacts[contact.id]
					if (upsert) {
						logger.debug({ contactId: contact.id }, 'absorbed contact upsert in contact set')
					}
				}

				if (upsert) {
					upsert = Object.assign(upsert, trimUndefined(contact))
				} else {
					upsert = contact
					data.contactUpserts[contact.id] = upsert
				}

				if (data.contactUpdates[contact.id]) {
					upsert = Object.assign(data.contactUpdates[contact.id]!, trimUndefined(contact)) as Contact
					delete data.contactUpdates[contact.id]
				}
			}

			break
		case 'contacts.update':
			const contactUpdates = eventData as BaileysEventMap['contacts.update']
			for (const update of contactUpdates) {
				const id = update.id!
				// merge into prior upsert
				const upsert = data.historySets.contacts[id] || data.contactUpserts[id]
				if (upsert) {
					Object.assign(upsert, update)
				} else {
					// merge into prior update
					const contactUpdate = data.contactUpdates[id] || {}
					data.contactUpdates[id] = Object.assign(contactUpdate, update)
				}
			}

			break
		case 'messages.upsert':
			const { messages, type } = eventData as BaileysEventMap['messages.upsert']
			for (const message of messages) {
				const key = stringifyMessageKey(message.key)
				let existing = data.messageUpserts[key]?.message
				if (!existing) {
					existing = data.historySets.messages[key]
					if (existing) {
						logger.debug({ messageId: key }, 'absorbed message upsert in message set')
					}
				}

				if (existing) {
					message.messageTimestamp = existing.messageTimestamp
				}

				if (data.messageUpdates[key]) {
					logger.debug('absorbed prior message update in message upsert')
					Object.assign(message, data.messageUpdates[key].update)
					delete data.messageUpdates[key]
				}

				if (data.historySets.messages[key]) {
					data.historySets.messages[key] = message
				} else {
					data.messageUpserts[key] = {
						message,
						type: type === 'notify' || data.messageUpserts[key]?.type === 'notify' ? 'notify' : type
					}
				}
			}

			break
		case 'messages.update':
			const msgUpdates = eventData as BaileysEventMap['messages.update']
			for (const { key, update } of msgUpdates) {
				const keyStr = stringifyMessageKey(key)
				const existing = data.historySets.messages[keyStr] || data.messageUpserts[keyStr]?.message
				if (existing) {
					Object.assign(existing, update)
					// if the message was received & read by us
					// the chat counter must have been incremented
					// so we need to decrement it
					if (update.status === WAMessageStatus.READ && !key.fromMe) {
						decrementChatReadCounterIfMsgDidUnread(existing)
					}
				} else {
					const msgUpdate = data.messageUpdates[keyStr] || { key, update: {} }
					Object.assign(msgUpdate.update, update)
					data.messageUpdates[keyStr] = msgUpdate
				}
			}

			break
		case 'messages.delete':
			const deleteData = eventData as BaileysEventMap['messages.delete']
			if ('keys' in deleteData) {
				const { keys } = deleteData
				for (const key of keys) {
					const keyStr = stringifyMessageKey(key)
					if (!data.messageDeletes[keyStr]) {
						data.messageDeletes[keyStr] = key
					}

					if (data.messageUpserts[keyStr]) {
						delete data.messageUpserts[keyStr]
					}

					if (data.messageUpdates[keyStr]) {
						delete data.messageUpdates[keyStr]
					}
				}
			} else {
				// TODO: add support
			}

			break
		case 'messages.reaction':
			const reactions = eventData as BaileysEventMap['messages.reaction']
			for (const { key, reaction } of reactions) {
				const keyStr = stringifyMessageKey(key)
				const existing = data.messageUpserts[keyStr]
				if (existing) {
					updateMessageWithReaction(existing.message, reaction)
				} else {
					data.messageReactions[keyStr] = data.messageReactions[keyStr] || { key, reactions: [] }
					updateMessageWithReaction(data.messageReactions[keyStr], reaction)
				}
			}

			break
		case 'message-receipt.update':
			const receipts = eventData as BaileysEventMap['message-receipt.update']
			for (const { key, receipt } of receipts) {
				const keyStr = stringifyMessageKey(key)
				const existing = data.messageUpserts[keyStr]
				if (existing) {
					updateMessageWithReceipt(existing.message, receipt)
				} else {
					data.messageReceipts[keyStr] = data.messageReceipts[keyStr] || { key, userReceipt: [] }
					updateMessageWithReceipt(data.messageReceipts[keyStr], receipt)
				}
			}

			break
		case 'groups.update':
			const groupUpdates = eventData as BaileysEventMap['groups.update']
			for (const update of groupUpdates) {
				const id = update.id!
				const groupUpdate = data.groupUpdates[id] || {}
				if (!data.groupUpdates[id]) {
					data.groupUpdates[id] = Object.assign(groupUpdate, update)
				}
			}

			break
		default:
			throw new Error(`"${event}" cannot be buffered`)
	}

	function decrementChatReadCounterIfMsgDidUnread(message: WAMessage) {
		// decrement chat unread counter
		// if the message has already been marked read by us
		const chatId = message.key.remoteJid!
		const chat = data.chatUpdates[chatId] || data.chatUpserts[chatId]
		if (
			isRealMessage(message) &&
			shouldIncrementChatUnread(message) &&
			typeof chat?.unreadCount === 'number' &&
			chat.unreadCount > 0
		) {
			logger.debug({ chatId: chat.id }, 'decrementing chat counter')
			chat.unreadCount -= 1
			if (chat.unreadCount === 0) {
				delete chat.unreadCount
			}
		}
	}
}

function consolidateEvents(data: BufferedEventData) {
	const map: BaileysEventData = {}

	if (!data.historySets.empty) {
		map['messaging-history.set'] = {
			chats: Object.values(data.historySets.chats),
			messages: Object.values(data.historySets.messages),
			contacts: Object.values(data.historySets.contacts),
			syncType: data.historySets.syncType,
			progress: data.historySets.progress,
			isLatest: data.historySets.isLatest,
			peerDataRequestSessionId: data.historySets.peerDataRequestSessionId
		}
	}

	const chatUpsertList = Object.values(data.chatUpserts)
	if (chatUpsertList.length) {
		map['chats.upsert'] = chatUpsertList
	}

	const chatUpdateList = Object.values(data.chatUpdates)
	if (chatUpdateList.length) {
		map['chats.update'] = chatUpdateList
	}

	const chatDeleteList = Array.from(data.chatDeletes)
	if (chatDeleteList.length) {
		map['chats.delete'] = chatDeleteList
	}

	const messageUpsertList = Object.values(data.messageUpserts)
	if (messageUpsertList.length) {
		const type = messageUpsertList[0]!.type
		map['messages.upsert'] = {
			messages: messageUpsertList.map(m => m.message),
			type
		}
	}

	const messageUpdateList = Object.values(data.messageUpdates)
	if (messageUpdateList.length) {
		map['messages.update'] = messageUpdateList
	}

	const messageDeleteList = Object.values(data.messageDeletes)
	if (messageDeleteList.length) {
		map['messages.delete'] = { keys: messageDeleteList }
	}

	const messageReactionList = Object.values(data.messageReactions).flatMap(({ key, reactions }) =>
		reactions.flatMap(reaction => ({ key, reaction }))
	)
	if (messageReactionList.length) {
		map['messages.reaction'] = messageReactionList
	}

	const messageReceiptList = Object.values(data.messageReceipts).flatMap(({ key, userReceipt }) =>
		userReceipt.flatMap(receipt => ({ key, receipt }))
	)
	if (messageReceiptList.length) {
		map['message-receipt.update'] = messageReceiptList
	}

	const contactUpsertList = Object.values(data.contactUpserts)
	if (contactUpsertList.length) {
		map['contacts.upsert'] = contactUpsertList
	}

	const contactUpdateList = Object.values(data.contactUpdates)
	if (contactUpdateList.length) {
		map['contacts.update'] = contactUpdateList
	}

	const groupUpdateList = Object.values(data.groupUpdates)
	if (groupUpdateList.length) {
		map['groups.update'] = groupUpdateList
	}

	return map
}

function concatChats<C extends Partial<Chat>>(a: C, b: Partial<Chat>) {
	if (
		b.unreadCount === null && // neutralize unread counter
		a.unreadCount! < 0
	) {
		a.unreadCount = undefined
		b.unreadCount = undefined
	}

	if (typeof a.unreadCount === 'number' && typeof b.unreadCount === 'number') {
		b = { ...b }
		if (b.unreadCount! >= 0) {
			b.unreadCount = Math.max(b.unreadCount!, 0) + Math.max(a.unreadCount, 0)
		}
	}

	return Object.assign(a, b)
}

const stringifyMessageKey = (key: WAMessageKey) => `${key.remoteJid},${key.id},${key.fromMe ? '1' : '0'}`
