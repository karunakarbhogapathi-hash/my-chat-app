package com.example.viewmodel

import android.util.Log
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.example.data.model.CallSession
import com.example.data.model.ChatMessage
import com.example.data.model.ChatRoom
import com.example.data.model.StatusPost
import com.example.data.model.UiState
import com.example.data.model.UserPrivateSettings
import com.example.data.model.UserProfile
import com.example.data.repository.ChatRepository
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch

sealed interface AppNotificationEvent {
    data class NewMessage(val notificationId: Int, val senderName: String, val previewText: String) : AppNotificationEvent
    data class IncomingCall(val callIdHash: Int, val callerName: String, val callType: String) : AppNotificationEvent
}

@OptIn(ExperimentalCoroutinesApi::class)
class MyChatViewModel(
    private val repository: ChatRepository,
    val currentUserId: String,
    initialDisplayName: String,
    initialEmail: String,
    initialPhotoUrl: String
) : ViewModel() {

    private val _snackbarMessage = MutableStateFlow<String?>(null)
    val snackbarMessage: StateFlow<String?> = _snackbarMessage.asStateFlow()

    private val _notificationEvents = MutableSharedFlow<AppNotificationEvent>(extraBufferCapacity = 8)
    val notificationEvents = _notificationEvents.asSharedFlow()

    private val _searchQuery = MutableStateFlow("")
    val searchQuery: StateFlow<String> = _searchQuery.asStateFlow()

    private val _selectedChatFilter = MutableStateFlow("all") // "all" | "groups" | "direct"
    val selectedChatFilter: StateFlow<String> = _selectedChatFilter.asStateFlow()

    private val _activeChatId = MutableStateFlow<String?>(null)
    val activeChatId: StateFlow<String?> = _activeChatId.asStateFlow()

    private val _activeCallId = MutableStateFlow<String?>(null)
    val activeCallId: StateFlow<String?> = _activeCallId.asStateFlow()

    private val _isBusy = MutableStateFlow(false)
    val isBusy: StateFlow<Boolean> = _isBusy.asStateFlow()

    private var seenChatUpdates = mutableMapOf<String, Long>()
    private var initializedChatSnapshots = false
    private var notifiedCallIds = mutableSetOf<String>()

    init {
        viewModelScope.launch {
            try {
                repository.ensureProfileBootstrapped(
                    displayName = initialDisplayName,
                    email = initialEmail,
                    photoUrl = initialPhotoUrl
                )
            } catch (e: Exception) {
                Log.w(TAG, "Profile bootstrap failed", e)
                _snackbarMessage.value = e.localizedMessage ?: "Could not initialize user profile"
            }
        }
    }

    val myProfile: StateFlow<UserProfile?> = repository.observeMyProfile(currentUserId)
        .catch { e ->
            Log.w(TAG, "Error observing myProfile", e)
            emit(null)
        }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS), null)

    val myPrivateSettings: StateFlow<UserPrivateSettings?> = repository.observeMyPrivateSettings(currentUserId)
        .catch { e ->
            Log.w(TAG, "Error observing private settings", e)
            emit(null)
        }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS), null)

    val profilesMap: StateFlow<Map<String, UserProfile>> = repository.observeDiscoverableProfiles()
        .map { list -> list.associateBy { it.userId.ifBlank { it.id } } }
        .catch { e ->
            Log.w(TAG, "Error observing profiles directory", e)
            emit(emptyMap())
        }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS), emptyMap())

    val chatsState: StateFlow<UiState<List<ChatRoom>>> = repository.observeMyChats(currentUserId)
        .onEach { chats ->
            val settings = myPrivateSettings.value
            val notificationsAllowed = settings?.notificationsEnabled != false
            if (!initializedChatSnapshots) {
                chats.forEach { room ->
                    seenChatUpdates[room.id] = room.updatedAt?.toDate()?.time ?: 0L
                }
                initializedChatSnapshots = true
            } else {
                for (room in chats) {
                    val updatedMs = room.updatedAt?.toDate()?.time ?: 0L
                    val prevMs = seenChatUpdates[room.id] ?: 0L
                    if (updatedMs > prevMs) {
                        seenChatUpdates[room.id] = updatedMs
                        if (notificationsAllowed &&
                            room.lastMessageSenderId.isNotBlank() &&
                            room.lastMessageSenderId != currentUserId &&
                            _activeChatId.value != room.id
                        ) {
                            _notificationEvents.tryEmit(
                                AppNotificationEvent.NewMessage(
                                    notificationId = room.id.hashCode(),
                                    senderName = room.lastMessageSenderName.ifBlank { room.title.ifBlank { "New Message" } },
                                    previewText = room.lastMessageText.ifBlank { "Sent a message" }
                                )
                            )
                        }
                    }
                }
            }
        }
        .map<List<ChatRoom>, UiState<List<ChatRoom>>> { UiState.Success(it) }
        .catch { e ->
            Log.w(TAG, "Error observing chats", e)
            emit(UiState.Error(e.localizedMessage ?: "Failed to load conversations"))
        }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS), UiState.Loading)

    val statusesState: StateFlow<UiState<List<StatusPost>>> = repository.observeContactStatuses()
        .map<List<StatusPost>, UiState<List<StatusPost>>> { UiState.Success(it) }
        .catch { e ->
            Log.w(TAG, "Error observing statuses", e)
            emit(UiState.Error(e.localizedMessage ?: "Failed to load status updates"))
        }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS), UiState.Loading)

    val callsState: StateFlow<UiState<List<CallSession>>> = repository.observeCalls(currentUserId)
        .onEach { calls ->
            val incomingRinging = calls.firstOrNull { call ->
                call.status == "ringing" && call.receiverId == currentUserId
            }
            if (incomingRinging != null) {
                if (_activeCallId.value == null) {
                    _activeCallId.value = incomingRinging.id
                }
                if (!notifiedCallIds.contains(incomingRinging.id) &&
                    myPrivateSettings.value?.callNotificationsEnabled != false
                ) {
                    notifiedCallIds.add(incomingRinging.id)
                    _notificationEvents.tryEmit(
                        AppNotificationEvent.IncomingCall(
                            callIdHash = incomingRinging.id.hashCode(),
                            callerName = incomingRinging.callerName,
                            callType = incomingRinging.callType
                        )
                    )
                }
            }
        }
        .map<List<CallSession>, UiState<List<CallSession>>> { UiState.Success(it) }
        .catch { e ->
            Log.w(TAG, "Error observing calls", e)
            emit(UiState.Error(e.localizedMessage ?: "Failed to load call history"))
        }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS), UiState.Loading)

    val activeChatRoom: StateFlow<ChatRoom?> = _activeChatId
        .flatMapLatest { chatId ->
            if (chatId.isNullOrBlank()) flowOf(null)
            else repository.observeChat(chatId).catch { emit(null) }
        }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS), null)

    val activeMessagesState: StateFlow<UiState<List<ChatMessage>>> = _activeChatId
        .flatMapLatest { chatId ->
            if (chatId.isNullOrBlank()) {
                flowOf(UiState.Success(emptyList()))
            } else {
                repository.observeMessages(chatId, currentUserId)
                    .onEach { msgs ->
                        val receipts = myPrivateSettings.value?.readReceiptsEnabled != false
                        repository.markMessagesRead(chatId, msgs, receipts)
                    }
                    .map<List<ChatMessage>, UiState<List<ChatMessage>>> { UiState.Success(it) }
                    .catch { e ->
                        Log.w(TAG, "Error observing messages", e)
                        emit(UiState.Error(e.localizedMessage ?: "Failed to load messages"))
                    }
            }
        }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS), UiState.Loading)

    val activeCallSession: StateFlow<CallSession?> = combine(_activeCallId, callsState) { callId, state ->
        if (callId.isNullOrBlank()) return@combine null
        val list = (state as? UiState.Success)?.data ?: emptyList()
        list.firstOrNull { it.id == callId }
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(STOP_TIMEOUT_MS), null)

    fun setSearchQuery(query: String) {
        _searchQuery.value = query
    }

    fun setChatFilter(filter: String) {
        _selectedChatFilter.value = filter
    }

    fun clearSnackbar() {
        _snackbarMessage.value = null
    }

    fun openChat(chatId: String) {
        _activeChatId.value = chatId
    }

    fun closeActiveChat() {
        val currentRoom = activeChatRoom.value
        val chatId = _activeChatId.value
        if (chatId != null && currentRoom != null && currentRoom.typingUserIds.contains(currentUserId)) {
            viewModelScope.launch {
                repository.setTypingIndicator(chatId, currentRoom.typingUserIds, false)
            }
        }
        _activeChatId.value = null
    }

    fun setOnlinePresence(isOnline: Boolean) {
        viewModelScope.launch {
            repository.updatePresence(isOnline)
        }
    }

    fun startOrOpenDirectChat(contact: UserProfile, onReady: (String) -> Unit) {
        val targetId = contact.userId.ifBlank { contact.id }
        if (targetId.isBlank() || targetId == currentUserId) return
        _isBusy.value = true
        viewModelScope.launch {
            try {
                val chatId = repository.getOrCreateDirectChat(targetId, contact.displayName)
                _isBusy.value = false
                _activeChatId.value = chatId
                onReady(chatId)
            } catch (e: Exception) {
                _isBusy.value = false
                _snackbarMessage.value = e.localizedMessage ?: "Unable to start chat"
            }
        }
    }

    fun createGroupChat(
        title: String,
        description: String,
        photoData: String,
        memberIds: List<String>,
        onReady: (String) -> Unit
    ) {
        _isBusy.value = true
        viewModelScope.launch {
            try {
                val chatId = repository.createGroupChat(title, description, photoData, memberIds)
                _isBusy.value = false
                _activeChatId.value = chatId
                onReady(chatId)
            } catch (e: Exception) {
                _isBusy.value = false
                _snackbarMessage.value = e.localizedMessage ?: "Unable to create group"
            }
        }
    }

    fun updateGroupDetails(
        chatId: String,
        title: String,
        description: String,
        photoData: String,
        participantIds: List<String>,
        adminIds: List<String>
    ) {
        viewModelScope.launch {
            try {
                repository.updateGroupDetails(chatId, title, description, photoData, participantIds, adminIds)
                _snackbarMessage.value = "Group updated"
            } catch (e: Exception) {
                _snackbarMessage.value = e.localizedMessage ?: "Failed to update group"
            }
        }
    }

    fun onTypingChanged(isTyping: Boolean) {
        val room = activeChatRoom.value ?: return
        viewModelScope.launch {
            repository.setTypingIndicator(room.id, room.typingUserIds, isTyping)
        }
    }

    fun sendMessage(
        type: String,
        text: String,
        imageData: String = "",
        replyToMessageId: String = "",
        replyPreviewText: String = ""
    ) {
        val room = activeChatRoom.value ?: return
        val senderName = myProfile.value?.displayName?.ifBlank { "User" } ?: "User"
        if (type != "image" && text.isBlank()) return
        viewModelScope.launch {
            try {
                repository.sendMessage(
                    chatId = room.id,
                    participantIds = room.participantIds,
                    senderName = senderName,
                    type = type,
                    text = text,
                    imageData = imageData,
                    replyToMessageId = replyToMessageId,
                    replyPreviewText = replyPreviewText,
                    currentTypingIds = room.typingUserIds
                )
            } catch (e: Exception) {
                _snackbarMessage.value = e.localizedMessage ?: "Message failed to send"
            }
        }
    }

    fun deleteMessage(messageId: String) {
        val chatId = _activeChatId.value ?: return
        viewModelScope.launch {
            try {
                repository.deleteMessage(chatId, messageId)
            } catch (e: Exception) {
                _snackbarMessage.value = e.localizedMessage ?: "Could not delete message"
            }
        }
    }

    fun postStatus(
        mediaType: String,
        caption: String,
        imageData: String = "",
        backgroundColorHex: String = "#075E54",
        visibility: String = "contacts"
    ) {
        val profile = myProfile.value
        val authorName = profile?.displayName?.ifBlank { "User" } ?: "User"
        val authorPhoto = profile?.photoData ?: ""
        _isBusy.value = true
        viewModelScope.launch {
            try {
                repository.postStatus(
                    authorName = authorName,
                    authorPhotoData = authorPhoto,
                    mediaType = mediaType,
                    caption = caption,
                    imageData = imageData,
                    backgroundColorHex = backgroundColorHex,
                    visibility = visibility
                )
                _isBusy.value = false
                _snackbarMessage.value = "Status shared!"
            } catch (e: Exception) {
                _isBusy.value = false
                _snackbarMessage.value = e.localizedMessage ?: "Could not post status"
            }
        }
    }

    fun markStatusViewed(status: StatusPost) {
        viewModelScope.launch {
            repository.markStatusViewed(status)
        }
    }

    fun deleteStatus(statusId: String) {
        viewModelScope.launch {
            try {
                repository.deleteStatus(statusId)
                _snackbarMessage.value = "Status deleted"
            } catch (e: Exception) {
                _snackbarMessage.value = e.localizedMessage ?: "Could not delete status"
            }
        }
    }

    fun initiateCall(receiver: UserProfile, callType: String) {
        val targetId = receiver.userId.ifBlank { receiver.id }
        if (targetId.isBlank() || targetId == currentUserId) return
        val caller = myProfile.value
        viewModelScope.launch {
            try {
                val callId = repository.startCall(
                    callerName = caller?.displayName?.ifBlank { "User" } ?: "User",
                    callerPhotoData = caller?.photoData ?: "",
                    receiverId = targetId,
                    receiverName = receiver.displayName.ifBlank { "Contact" },
                    receiverPhotoData = receiver.photoData,
                    callType = callType
                )
                _activeCallId.value = callId
            } catch (e: Exception) {
                _snackbarMessage.value = e.localizedMessage ?: "Unable to place call"
            }
        }
    }

    fun answerIncomingCall(call: CallSession) {
        viewModelScope.launch {
            try {
                repository.respondToCall(call.id, accept = true, isVideoCall = call.callType == "video")
            } catch (e: Exception) {
                _snackbarMessage.value = e.localizedMessage ?: "Failed to answer call"
            }
        }
    }

    fun declineIncomingCall(call: CallSession) {
        viewModelScope.launch {
            try {
                repository.respondToCall(call.id, accept = false, isVideoCall = false)
                if (_activeCallId.value == call.id) {
                    _activeCallId.value = null
                }
            } catch (e: Exception) {
                _activeCallId.value = null
            }
        }
    }

    fun updateCallMedia(call: CallSession, isMuted: Boolean, isVideoEnabled: Boolean) {
        val isCaller = call.callerId == currentUserId
        viewModelScope.launch {
            repository.updateCallMediaState(call.id, isCaller, isMuted, isVideoEnabled)
        }
    }

    fun endActiveCall(call: CallSession, durationSeconds: Long) {
        viewModelScope.launch {
            repository.endCall(call.id, call.status, durationSeconds)
            if (_activeCallId.value == call.id) {
                _activeCallId.value = null
            }
        }
    }

    fun dismissFinishedCallOverlay() {
        _activeCallId.value = null
    }

    fun saveProfileAndPrivacy(
        displayName: String,
        handle: String,
        about: String,
        photoData: String,
        showLastSeen: Boolean,
        isDiscoverable: Boolean,
        phoneNumber: String,
        readReceiptsEnabled: Boolean,
        notificationsEnabled: Boolean,
        callNotificationsEnabled: Boolean,
        darkModePreference: String
    ) {
        _isBusy.value = true
        viewModelScope.launch {
            try {
                repository.updateProfile(
                    displayName = displayName,
                    handle = handle,
                    about = about,
                    photoData = photoData,
                    showLastSeen = showLastSeen,
                    isDiscoverable = isDiscoverable
                )
                repository.updatePrivateSettings(
                    phoneNumber = phoneNumber,
                    readReceiptsEnabled = readReceiptsEnabled,
                    notificationsEnabled = notificationsEnabled,
                    callNotificationsEnabled = callNotificationsEnabled,
                    darkModePreference = darkModePreference
                )
                _isBusy.value = false
                _snackbarMessage.value = "Settings saved"
            } catch (e: Exception) {
                _isBusy.value = false
                _snackbarMessage.value = e.localizedMessage ?: "Could not save settings"
            }
        }
    }

    private companion object {
        const val TAG = "MyChatVM"
        const val STOP_TIMEOUT_MS = 5000L
    }
}
