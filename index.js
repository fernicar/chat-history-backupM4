// Chat Auto Backup Plugin - Automatically saves and restores the last three chat records
// Main Features:
// 1. Automatically save recent chat records to IndexedDB (triggered by events, distinguishing between immediate and debounced)
// 2. Display saved records on the plugin page
// 3. Provide restore functionality to restore saved chat records to new chats
// 4. Utilize Web Worker for optimizing deep copy performance

import {
    getContext,
    renderExtensionTemplateAsync,
    extension_settings,
} from '../../../extensions.js';

import {
    // --- Core Application Functions ---
    saveSettingsDebounced,
    eventSource,
    event_types,
    selectCharacterById,    // Used to select characters
    doNewChat,              // Used to create a new chat
    printMessages,          // Used to refresh chat UI
    scrollChatToBottom,     // Used to scroll to the bottom
    updateChatMetadata,     // Used to update chat metadata
    saveChatConditional,    // Used to save chat conditionally
    saveChat,               // Used to force save chat by the plugin
    characters,             // Need to access the character list to find index
    getThumbnailUrl,        // May need to get avatar URL (though backup should contain it)
    // --- Other potentially needed functions ---
    // clearChat, // May not be needed, doNewChat should handle it
    // getCharacters, // May need to update after switching characters? selectCharacterById should handle it internally
} from '../../../../script.js';

import {
    // --- Group Chat Functions ---
    select_group_chats,     // Used to select group chats
    // getGroupChat, // May not be needed, select_group_chats should handle it
} from '../../../group-chats.js';

// Plugin Name and Settings Initialization
const PLUGIN_NAME = 'chat-history-backup';
const DEFAULT_SETTINGS = {
    maxTotalBackups: 3,        // Maximum number of backups to retain for the entire system
    backupDebounceDelay: 1000, // Debounce delay time (milliseconds)
    debug: true,               // Debug mode
};

// IndexedDB Database Name and Version
const DB_NAME = 'ST_ChatAutoBackup';
const DB_VERSION = 1;
const STORE_NAME = 'backups';

// Web Worker Instance (initialized later)
let backupWorker = null;
// Promises to track Worker requests
const workerPromises = {};
let workerRequestId = 0;

// Database Connection Pool - Implements Singleton Pattern
let dbConnection = null;

// Backup State Control
let isBackupInProgress = false; // Concurrency control flag
let backupTimeout = null;       // Debounce timer ID

// --- Deep Copy Logic (will be used in Worker and main thread) ---
const deepCopyLogicString = `
    const deepCopy = (obj) => {
        try {
            return structuredClone(obj);
        } catch (error) {
            try {
                return JSON.parse(JSON.stringify(obj));
            } catch (jsonError) {
                throw jsonError; // Throw error to let the main thread know
            }
        }
    };
`;

// --- Logging Function ---
function logDebug(...args) {
    const settings = extension_settings[PLUGIN_NAME];
    if (settings && settings.debug) {
        console.log(`[Chat Auto Backup][${new Date().toLocaleTimeString()}]`, ...args);
    }
}

// --- Settings Initialization ---
function initSettings() {
    console.log('[Chat Auto Backup] Initializing plugin settings');
    if (!extension_settings[PLUGIN_NAME]) {
        console.log('[Chat Auto Backup] Creating new plugin settings');
        extension_settings[PLUGIN_NAME] = { ...DEFAULT_SETTINGS };
    }

    // Ensure settings structure is complete
    const settings = extension_settings[PLUGIN_NAME];
    
    // Migrate from old settings if they exist
    if (settings.hasOwnProperty('maxBackupsPerChat') && !settings.hasOwnProperty('maxTotalBackups')) {
        settings.maxTotalBackups = 3; // Default value
        delete settings.maxBackupsPerChat; // Remove old setting
        console.log('[Chat Auto Backup] Migrating from old settings to new settings');
    }
    
    // Ensure all settings exist
    settings.maxTotalBackups = settings.maxTotalBackups ?? DEFAULT_SETTINGS.maxTotalBackups;
    settings.backupDebounceDelay = settings.backupDebounceDelay ?? DEFAULT_SETTINGS.backupDebounceDelay;
    settings.debug = settings.debug ?? DEFAULT_SETTINGS.debug;

    // Validate settings for reasonableness
    if (typeof settings.maxTotalBackups !== 'number' || settings.maxTotalBackups < 1) {
        console.log(`[Chat Auto Backup] Invalid max backups ${settings.maxTotalBackups}, resetting to default ${DEFAULT_SETTINGS.maxTotalBackups}`);
        settings.maxTotalBackups = DEFAULT_SETTINGS.maxTotalBackups;
    }
    
    if (typeof settings.backupDebounceDelay !== 'number' || settings.backupDebounceDelay < 300) {
        console.log(`[Chat Auto Backup] Invalid debounce delay ${settings.backupDebounceDelay}, resetting to default ${DEFAULT_SETTINGS.backupDebounceDelay}`);
        settings.backupDebounceDelay = DEFAULT_SETTINGS.backupDebounceDelay;
    }

    console.log('[Chat Auto Backup] Plugin settings initialization complete:', settings);
    return settings;
}

// --- IndexedDB Related Functions (Optimized Version) ---
// Initialize IndexedDB database
function initDatabase() {
    return new Promise((resolve, reject) => {
        logDebug('Initializing IndexedDB database');
        const request = indexedDB.open(DB_NAME, DB_VERSION);

        request.onerror = function(event) {
            console.error('[Chat Auto Backup] Failed to open database:', event.target.error);
            reject(event.target.error);
        };

        request.onsuccess = function(event) {
            const db = event.target.result;
            logDebug('Database opened successfully');
            resolve(db);
        };

        request.onupgradeneeded = function(event) {
            const db = event.target.result;
            console.log('[Chat Auto Backup] Database upgrading, creating object store');
            if (!db.objectStoreNames.contains(STORE_NAME)) {
                const store = db.createObjectStore(STORE_NAME, { keyPath: ['chatKey', 'timestamp'] });
                store.createIndex('chatKey', 'chatKey', { unique: false });
                console.log('[Chat Auto Backup] Created backup store and index');
            }
        };
    });
}

// Get database connection (Optimized Version - Using Connection Pool)
async function getDB() {
    try {
        // Check if existing connection is available
        if (dbConnection && dbConnection.readyState !== 'closed') {
            return dbConnection;
        }
        
        // Create new connection
        dbConnection = await initDatabase();
        return dbConnection;
    } catch (error) {
        console.error('[Chat Auto Backup] Failed to get database connection:', error);
        throw error;
    }
}

// Save backup to IndexedDB (Optimized Version)
async function saveBackupToDB(backup) {
    const db = await getDB();
    try {
        await new Promise((resolve, reject) => {
            const transaction = db.transaction([STORE_NAME], 'readwrite');
            
            transaction.oncomplete = () => {
                logDebug(`Backup saved to IndexedDB, key: [${backup.chatKey}, ${backup.timestamp}]`);
                resolve();
            };
            
            transaction.onerror = (event) => {
                console.error('[Chat Auto Backup] Backup transaction failed:', event.target.error);
                reject(event.target.error);
            };
            
            const store = transaction.objectStore(STORE_NAME);
            store.put(backup);
        });
    } catch (error) {
        console.error('[Chat Auto Backup] saveBackupToDB failed:', error);
        throw error;
    }
}

// Get all backups for a specific chat from IndexedDB (Optimized Version)
async function getBackupsForChat(chatKey) {
    const db = await getDB();
    try {
        return await new Promise((resolve, reject) => {
            const transaction = db.transaction([STORE_NAME], 'readonly');
            
            transaction.onerror = (event) => {
                console.error('[Chat Auto Backup] Backup retrieval transaction failed:', event.target.error);
                reject(event.target.error);
            };
            
            const store = transaction.objectStore(STORE_NAME);
            const index = store.index('chatKey');
            const request = index.getAll(chatKey);
            
            request.onsuccess = () => {
                const backups = request.result || [];
                logDebug(`Retrieved ${backups.length} backups from IndexedDB for chatKey: ${chatKey}`);
                resolve(backups);
            };
            
            request.onerror = (event) => {
                console.error('[Chat Auto Backup] Failed to retrieve backups:', event.target.error);
                reject(event.target.error);
            };
        });
    } catch (error) {
        console.error('[Chat Auto Backup] getBackupsForChat failed:', error);
        return []; // Return empty array on error
    }
}

// Get all backups from IndexedDB (Optimized Version)
async function getAllBackups() {
    const db = await getDB();
    try {
        return await new Promise((resolve, reject) => {
            const transaction = db.transaction([STORE_NAME], 'readonly');
            
            transaction.onerror = (event) => {
                console.error('[Chat Auto Backup] All backups retrieval transaction failed:', event.target.error);
                reject(event.target.error);
            };
            
            const store = transaction.objectStore(STORE_NAME);
            const request = store.getAll();
            
            request.onsuccess = () => {
                const backups = request.result || [];
                logDebug(`Retrieved a total of ${backups.length} backups from IndexedDB`);
                resolve(backups);
            };
            
            request.onerror = (event) => {
                console.error('[Chat Auto Backup] Failed to retrieve all backups:', event.target.error);
                reject(event.target.error);
            };
        });
    } catch (error) {
        console.error('[Chat Auto Backup] getAllBackups failed:', error);
        return [];
    }
}

// Get all backup keys from IndexedDB (Optimized for cleanup logic)
async function getAllBackupKeys() {
    const db = await getDB();
    try {
        return await new Promise((resolve, reject) => {
            const transaction = db.transaction([STORE_NAME], 'readonly');

            transaction.onerror = (event) => {
                console.error('[Chat Auto Backup] All backup keys retrieval transaction failed:', event.target.error);
                reject(event.target.error);
            };

            const store = transaction.objectStore(STORE_NAME);
            // Use getAllKeys() to get only the primary keys
            const request = store.getAllKeys();

            request.onsuccess = () => {
                // Returns an array of keys, where each key is ['chatKey', timestamp]
                const keys = request.result || [];
                logDebug(`Retrieved a total of ${keys.length} backup primary keys from IndexedDB`);
                resolve(keys);
            };

            request.onerror = (event) => {
                console.error('[Chat Auto Backup] Failed to retrieve all backup keys:', event.target.error);
                reject(event.target.error);
            };
        });
    } catch (error) {
        console.error('[Chat Auto Backup] getAllBackupKeys failed:', error);
        return []; // Return empty array on error
    }
} 

// Delete a specific backup from IndexedDB (Optimized Version)
async function deleteBackup(chatKey, timestamp) {
    const db = await getDB();
    try {
        await new Promise((resolve, reject) => {
            const transaction = db.transaction([STORE_NAME], 'readwrite');
            
            transaction.oncomplete = () => {
                logDebug(`Backup deleted from IndexedDB, key: [${chatKey}, ${timestamp}]`);
                resolve();
            };
            
            transaction.onerror = (event) => {
                console.error('[Chat Auto Backup] Backup deletion transaction failed:', event.target.error);
                reject(event.target.error);
            };
            
            const store = transaction.objectStore(STORE_NAME);
            store.delete([chatKey, timestamp]);
        });
    } catch (error) {
        console.error('[Chat Auto Backup] deleteBackup failed:', error);
        throw error;
    }
}

// --- Chat Information Retrieval (Remains Unchanged) ---
function getCurrentChatKey() {
    const context = getContext();
    logDebug('Getting current chat identifier, context:',
        {groupId: context.groupId, characterId: context.characterId, chatId: context.chatId});
    if (context.groupId) {
        const key = `group_${context.groupId}_${context.chatId}`;
        logDebug('Current chat is a group chat, chatKey:', key);
        return key;
    } else if (context.characterId !== undefined && context.chatId) { // Ensure chatId exists
        const key = `char_${context.characterId}_${context.chatId}`;
        logDebug('Current chat is a character chat, chatKey:', key);
        return key;
    }
    console.warn('[Chat Auto Backup] Could not obtain a valid identifier for the current chat (character/group not selected or no active chat)');
    return null;
}

function getCurrentChatInfo() {
    const context = getContext();
    let chatName = 'Current Chat', entityName = 'Unknown';

    if (context.groupId) {
        const group = context.groups?.find(g => g.id === context.groupId);
        entityName = group ? group.name : `Group ${context.groupId}`;
        chatName = context.chatId || 'New Chat'; // Use a more explicit default name
        logDebug('Retrieved group chat information:', {entityName, chatName});
    } else if (context.characterId !== undefined) {
        entityName = context.name2 || `Character ${context.characterId}`;
        const character = context.characters?.[context.characterId];
        if (character && context.chatId) {
             // Chat filename might contain paths, take only the last part
             const chatFile = character.chat || context.chatId;
             chatName = chatFile.substring(chatFile.lastIndexOf('/') + 1).replace('.jsonl', '');
        } else {
            chatName = context.chatId || 'New Chat';
        }
        logDebug('Retrieved character chat information:', {entityName, chatName});
    } else {
        console.warn('[Chat Auto Backup] Could not retrieve chat entity information, using default values');
    }

    return { entityName, chatName };
}

// --- Web Worker Communication ---
// Send data to Worker and return a Promise containing the deep-copied data
function performDeepCopyInWorker(chat, metadata) {
    return new Promise((resolve, reject) => {
        if (!backupWorker) {
            return reject(new Error("Backup worker not initialized."));
        }

        const currentRequestId = ++workerRequestId;
        workerPromises[currentRequestId] = { resolve, reject };

        logDebug(`[Main Thread] Sending data to Worker (ID: ${currentRequestId}), Chat length: ${chat?.length}`);
        try {
             // Send only the data that needs copying to reduce serialization overhead
            backupWorker.postMessage({
                id: currentRequestId,
                payload: { chat, metadata }
            });
        } catch (error) {
             console.error(`[Main Thread] Failed to send message to Worker (ID: ${currentRequestId}):`, error);
             delete workerPromises[currentRequestId];
             reject(error);
        }
    });
}

// --- Core Backup Logic Encapsulation (Receives Specific Data) ---
async function executeBackupLogic_Core(chat, chat_metadata_to_backup, settings) {
    const currentTimestamp = Date.now();
    logDebug(`(Encapsulated) Starting core backup logic @ ${new Date(currentTimestamp).toLocaleTimeString()}`);

    // 1. Pre-checks (using passed data instead of getContext())
    const chatKey = getCurrentChatKey(); // This still needs to get the current chatKey
    if (!chatKey) {
        console.warn('[Chat Auto Backup] (Encapsulated) No valid chat identifier');
        return false;
    }

    const { entityName, chatName } = getCurrentChatInfo();
    const lastMsgIndex = chat.length - 1;
    const lastMessage = chat[lastMsgIndex];
    const lastMessagePreview = lastMessage?.mes?.substring(0, 100) || '(Empty Message)';

    logDebug(`(Encapsulated) Preparing to back up chat: ${entityName} - ${chatName}, Message count: ${chat.length}, Last Message ID: ${lastMsgIndex}`);
    // *** Log the state of passed metadata for debugging ***
    logDebug(`(Encapsulated) State of metadata for backup:`, chat_metadata_to_backup);

    try {
        // 2. Use Worker for deep copy (using passed chat and chat_metadata_to_backup)
        let copiedChat, copiedMetadata;
        if (backupWorker) {
            try {
                console.time('[Chat Auto Backup] Web Worker Deep Copy Time');
                logDebug('(Encapsulated) Requesting Worker to perform deep copy...');
                const result = await performDeepCopyInWorker(chat, chat_metadata_to_backup); // Use passed data
                copiedChat = result.chat;
                copiedMetadata = result.metadata;
                console.timeEnd('[Chat Auto Backup] Web Worker Deep Copy Time');
                logDebug('(Encapsulated) Received copied data from Worker');
            } catch(workerError) {
                 // ... Main thread fallback logic, using passed chat and chat_metadata_to_backup ...
                 console.error('[Chat Auto Backup] (Encapsulated) Worker deep copy failed, attempting main thread execution:', workerError);
                  console.time('[Chat Auto Backup] Main Thread Deep Copy Time (Worker Failed)');
                  try {
                      copiedChat = structuredClone(chat);
                      copiedMetadata = structuredClone(chat_metadata_to_backup); // Use passed data
                  } catch (structuredCloneError) {
                     try {
                         copiedChat = JSON.parse(JSON.stringify(chat));
                         copiedMetadata = JSON.parse(JSON.stringify(chat_metadata_to_backup)); // Use passed data
                     } catch (jsonError) {
                         console.error('[Chat Auto Backup] (Encapsulated) Main thread deep copy also failed:', jsonError);
                         throw new Error("Failed to complete deep copy of chat data");
                     }
                  }
                  console.timeEnd('[Chat Auto Backup] Main Thread Deep Copy Time (Worker Failed)');
            }
        } else {
            // Worker is not available, perform directly on main thread (using passed chat and chat_metadata_to_backup)
            console.time('[Chat Auto Backup] Main Thread Deep Copy Time (No Worker)');
             try {
                 copiedChat = structuredClone(chat);
                 copiedMetadata = structuredClone(chat_metadata_to_backup); // Use passed data
             } catch (structuredCloneError) {
                try {
                    copiedChat = JSON.parse(JSON.stringify(chat));
                    copiedMetadata = JSON.parse(JSON.stringify(chat_metadata_to_backup)); // Use passed data
                } catch (jsonError) {
                    console.error('[Chat Auto Backup] (Encapsulated) Main thread deep copy failed:', jsonError);
                    throw new Error("Failed to complete deep copy of chat data");
                }
             }
            console.timeEnd('[Chat Auto Backup] Main Thread Deep Copy Time (No Worker)');
        }

        if (!copiedChat) {
             throw new Error("Failed to obtain a valid chat data copy");
        }

        // 3. Construct backup object
        const backup = {
            timestamp: currentTimestamp,
            chatKey,
            entityName,
            chatName,
            lastMessageId: lastMsgIndex,
            lastMessagePreview,
            chat: copiedChat,
            metadata: copiedMetadata || {} // Ensure metadata is always an object
        };

        // 4. Check if the current chat already has a backup based on the last message ID (avoid exact duplicate backups)
        const existingBackups = await getBackupsForChat(chatKey); // Get backups for the current chat

        // 5. Check for duplicates and handle (based on lastMessageId)
        const existingBackupIndex = existingBackups.findIndex(b => b.lastMessageId === lastMsgIndex);
        let needsSave = true;

        if (existingBackupIndex !== -1) {
             // If a backup with the same lastMessageId is found
            const existingTimestamp = existingBackups[existingBackupIndex].timestamp;
            if (backup.timestamp > existingTimestamp) {
                // New backup is newer, delete the old backup with the same ID
                logDebug(`(Encapsulated) Found older backup with same last message ID (${lastMsgIndex}) (timestamp ${existingTimestamp}), deleting old backup to save new one (timestamp ${backup.timestamp})`);
                await deleteBackup(chatKey, existingTimestamp);
                // Note: No need to splice from existingBackups array, as it's no longer used for global cleanup
            } else {
                // Older backup or same timestamp, skip this save
                logDebug(`(Encapsulated) Found backup with same last message ID (${lastMsgIndex}) and newer or same timestamp (timestamp ${existingTimestamp} vs ${backup.timestamp}), skipping save`);
                needsSave = false;
            }
        }

        if (!needsSave) {
            logDebug('(Encapsulated) Backup already exists or no update needed (based on lastMessageId and timestamp comparison), skipping save and global cleanup steps');
            return false; // No need to save, return false
        }

        // 6. Save the new backup to IndexedDB
        await saveBackupToDB(backup);
        logDebug(`(Encapsulated) New backup saved: [${chatKey}, ${backup.timestamp}]`);

        // --- Optimized Cleanup Logic ---
        // 7. Get *primary keys* of all backups and limit the total count
        logDebug(`(Encapsulated) Getting primary keys of all backups to check against system limit (${settings.maxTotalBackups})`);
        const allBackupKeys = await getAllBackupKeys(); // Call new function to get only keys

        if (allBackupKeys.length > settings.maxTotalBackups) {
            logDebug(`(Encapsulated) Total backup count (${allBackupKeys.length}) exceeds system limit (${settings.maxTotalBackups})`);

            // Sort keys by timestamp in ascending order
            // This places the keys of the oldest backups at the beginning of the array
            allBackupKeys.sort((a, b) => a[1] - b[1]); // a[1] = timestamp, b[1] = timestamp

            const numToDelete = allBackupKeys.length - settings.maxTotalBackups;
            // Get the first numToDelete keys from the array, which are the keys of the oldest backups to be deleted
            const keysToDelete = allBackupKeys.slice(0, numToDelete);

            logDebug(`(Encapsulated) Preparing to delete ${keysToDelete.length} oldest backups (based on keys)`);

            // Delete in parallel using Promise.all
            await Promise.all(keysToDelete.map(key => {
                const oldChatKey = key[0];
                const oldTimestamp = key[1];
                logDebug(`(Encapsulated) Deleting old backup (based on key): chatKey=${oldChatKey}, timestamp=${new Date(oldTimestamp).toLocaleString()}`);
                // Call deleteBackup, which accepts chatKey and timestamp
                return deleteBackup(oldChatKey, oldTimestamp);
            }));
            logDebug(`(Encapsulated) ${keysToDelete.length} oldest backups deleted`);
        } else {
            logDebug(`(Encapsulated) Total backup count (${allBackupKeys.length}) is within the limit (${settings.maxTotalBackups}), no cleanup needed`);
        }
        // --- Cleanup Logic Ends ---

        // 8. UI Notification
        logDebug(`(Encapsulated) Successfully completed chat backup and potential cleanup: ${entityName} - ${chatName}`);

        return true; // Indicates backup was successful (or skipped without error)

    } catch (error) {
        console.error('[Chat Auto Backup] (Encapsulated) Critical error during backup or cleanup:', error);
        throw error; // Throw error for the caller to handle notifications
    }
}

// --- Conditional Backup Function (Similar to saveChatConditional) ---
async function performBackupConditional() {
    if (isBackupInProgress) {
        logDebug('Backup already in progress, skipping this request');
        return;
    }

    // Get current settings, including debounce delay, in case they are modified during the delay
    const currentSettings = extension_settings[PLUGIN_NAME];
    if (!currentSettings) {
        console.error('[Chat Auto Backup] Could not retrieve current settings, aborting backup');
        return;
    }

    logDebug('Executing conditional backup (performBackupConditional)');
    clearTimeout(backupTimeout); // Cancel any pending debounced backups
    backupTimeout = null;

    try {
        logDebug('Attempting to call saveChatConditional() to refresh metadata...');
        console.log('[Chat Auto Backup] Before saveChatConditional', getContext().chatMetadata);
        await saveChatConditional();
        await new Promise(resolve => setTimeout(resolve, 100)); // Short delay
        logDebug('saveChatConditional() call completed, proceeding to get context');
        console.log('[Chat Auto Backup] After saveChatConditional', getContext().chatMetadata);
    } catch (e) {
        console.warn('[Chat Auto Backup] Error calling saveChatConditional (possibly harmless):', e);
    }

    const context = getContext();
    const chatKey = getCurrentChatKey();

    if (!chatKey) {
        logDebug('Could not get valid chat identifier (after saveChatConditional), aborting backup');
        // Log cancellation details, but use the correct property name for checking
        console.warn('[Chat Auto Backup] Cancellation Details (No ChatKey):', {
             contextDefined: !!context,
             // Note: Checking context.chatMetadata here
             chatMetadataDefined: !!context?.chatMetadata,
             sheetsDefined: !!context?.chatMetadata?.sheets,
             isSheetsArray: Array.isArray(context?.chatMetadata?.sheets),
             sheetsLength: context?.chatMetadata?.sheets?.length,
             condition1: !context?.chatMetadata, // Check for !context.chatMetadata
             condition2: !context?.chatMetadata?.sheets, // Check for !context.chatMetadata?.sheets
             condition3: context?.chatMetadata?.sheets?.length === 0 // Check for length
         });
        return false;
    }
    if (!context.chatMetadata) { // <--- Modified here! Changed from chat_metadata to chatMetadata
        console.warn('[Chat Auto Backup] Invalid chatMetadata (after saveChatConditional), aborting backup');
        // Log cancellation details
        console.warn('[Chat Auto Backup] Cancellation Details (chatMetadata Invalid):', {
            contextDefined: !!context,
            chatMetadataDefined: !!context?.chatMetadata,
            sheetsDefined: !!context?.chatMetadata?.sheets,
            isSheetsArray: Array.isArray(context?.chatMetadata?.sheets),
            sheetsLength: context?.chatMetadata?.sheets?.length,
            condition1: !context?.chatMetadata, // Check for !context.chatMetadata
            condition2: !context?.chatMetadata?.sheets, // Check for !context.chatMetadata?.sheets
            condition3: context?.chatMetadata?.sheets?.length === 0 // Check for length
        });
        return false;
    }
    // Check if context.chatMetadata.sheets exists and is not empty
    if (!context.chatMetadata.sheets || context.chatMetadata.sheets.length === 0) { // <--- Modified here! Changed from chat_metadata to chatMetadata
        console.warn('[Chat Auto Backup] Invalid or empty chatMetadata.sheets (after saveChatConditional), aborting backup');
        // Log cancellation details
        console.warn('[Chat Auto Backup] Cancellation Details (sheets Invalid/Empty):', {
            contextDefined: !!context,
            chatMetadataDefined: !!context?.chatMetadata,
            sheetsDefined: !!context?.chatMetadata?.sheets,
            isSheetsArray: Array.isArray(context?.chatMetadata?.sheets),
            sheetsLength: context?.chatMetadata?.sheets?.length,
            condition1: !context?.chatMetadata, // Check for !context.chatMetadata
            condition2: !context?.chatMetadata?.sheets, // Check for !context.chatMetadata?.sheets
            condition3: context?.chatMetadata?.sheets?.length === 0 // Check for length
        });
        return false;
    }

    isBackupInProgress = true;
    logDebug('Setting backup lock');
    try {
        // Now context.chatMetadata should contain the correct data
        const { chat } = context; // The chat property name is correct
        const chat_metadata_to_backup = context.chatMetadata; // <--- Use the correct property name to get metadata for backup
        const success = await executeBackupLogic_Core(chat, chat_metadata_to_backup, currentSettings); // Pass the correct metadata
        if (success) {
            await updateBackupsList();
        }
        return success;
    } catch (error) {
        console.error('[Chat Auto Backup] Conditional backup execution failed:', error);
        toastr.error(`Backup failed: ${error.message || 'Unknown error'}`, 'Chat Auto Backup');
        return false;
    } finally {
        isBackupInProgress = false;
        logDebug('Releasing backup lock');
    }
}

// --- Debounced Backup Function (Similar to saveChatDebounced) ---
function performBackupDebounced() {
    // Get context and settings at the time of scheduling
    const scheduledChatKey = getCurrentChatKey();
    const currentSettings = extension_settings[PLUGIN_NAME];

    if (!scheduledChatKey) {
        logDebug('Could not get ChatKey when scheduling debounced backup, aborting');
        clearTimeout(backupTimeout);
        backupTimeout = null;
        return;
    }
    
    if (!currentSettings || typeof currentSettings.backupDebounceDelay !== 'number') {
        console.error('[Chat Auto Backup] Could not get valid debounce delay settings, cancelling debounce');
        clearTimeout(backupTimeout);
        backupTimeout = null;
        return;
    }
    
    const delay = currentSettings.backupDebounceDelay; // Use the delay from current settings

    logDebug(`Scheduling debounced backup (delay ${delay}ms), targeting ChatKey: ${scheduledChatKey}`);
    clearTimeout(backupTimeout); // Clear any existing timers

    backupTimeout = setTimeout(async () => {
        const currentChatKey = getCurrentChatKey(); // Get ChatKey at execution time

        // Crucial check: Context verification
        if (currentChatKey !== scheduledChatKey) {
            logDebug(`Context has changed (current: ${currentChatKey}, scheduled at: ${scheduledChatKey}), cancelling this debounced backup`);
            backupTimeout = null;
            return; // Abort backup
        }

        logDebug(`Executing delayed backup operation (from debounce), ChatKey: ${currentChatKey}`);
        // Perform conditional backup only if the context matches
        await performBackupConditional();
        backupTimeout = null; // Clear the timer ID
    }, delay);
}

// --- Manual Backup ---
async function performManualBackup() {
    console.log('[Chat Auto Backup] Executing manual backup (calling conditional function)');
    await performBackupConditional(); // Manual backup also goes through conditional checks and locking
    toastr.success('Current chat has been manually backed up', 'Chat Auto Backup');
}

// --- Restore Logic ---
// restoreBackup function inside index.js (Review and Optimize)
async function restoreBackup(backupData) {
    // --- Entry Point and Basic Information Extraction ---
    console.log('[Chat Auto Backup] Starting backup restoration:', backupData); // <-- Added log
    const isGroup = backupData.chatKey.startsWith('group_');
    const entityIdMatch = backupData.chatKey.match(
        isGroup
        ? /group_(\w+)_/
        : /^char_(\d+)/
    );
    let entityId = entityIdMatch ? entityIdMatch[1] : null;
    let targetCharIndex = -1;

    if (!entityId) {
        console.error('[Chat Auto Backup] Could not extract character/group ID from backup data:', backupData.chatKey); // <-- Using error
        toastr.error('Could not identify the character/group ID associated with the backup');
        return false;
    }

    console.log(`[Chat Auto Backup] Restore Target: ${isGroup ? 'Group' : 'Character'} ID/Identifier: ${entityId}`); // <-- Added log

    // ... (Code remains unchanged) ...

    try {
        // --- Step 1: Switch Context ---
        const initialContext = getContext();
        console.log('[Chat Auto Backup] Step 1 - Context before switch:', { // <-- Logging pre-switch state
            groupId: initialContext.groupId,
            characterId: initialContext.characterId,
            chatId: initialContext.chatId
        });
        const needsContextSwitch = (isGroup && initialContext.groupId !== entityId) ||
                                   (!isGroup && String(initialContext.characterId) !== entityId);

        if (needsContextSwitch) {
            try {
                console.log('[Chat Auto Backup] Step 1: Context switch required, initiating switch...'); // <-- Added log
                if (isGroup) {
                    await select_group_chats(entityId);
                } else {
                    await selectCharacterById(entityToRestore.charIndex, { switchMenu: false });
                }
                await new Promise(resolve => setTimeout(resolve, 500));
                console.log('[Chat Auto Backup] Step 1: Context switch complete. Current context:', { // <-- Logging post-switch state
                    groupId: getContext().groupId,
                    characterId: getContext().characterId,
                    chatId: getContext().chatId
                });
            } catch (switchError) {
                console.error('[Chat Auto Backup] Step 1 Failed: Failed to switch character/group:', switchError);
                toastr.error(`Failed to switch context: ${switchError.message || switchError}`);
                return false;
            }
        } else {
            console.log('[Chat Auto Backup] Step 1: Already in the target context, skipping switch'); // <-- Added log
        }

        // --- Step 2: Create New Chat ---
        let originalChatIdBeforeNewChat = getContext().chatId;
        console.log('[Chat Auto Backup] Step 2: Starting new chat creation...'); // <-- Added log
        await doNewChat({ deleteCurrentChat: false });
        await new Promise(resolve => setTimeout(resolve, 1000));
        console.log('[Chat Auto Backup] Step 2: New chat creation complete'); // <-- Added log

        // --- Step 3: Get New Chat ID ---
        console.log('[Chat Auto Backup] Step 3: Getting new chat ID...'); // <-- Added log
        let contextAfterNewChat = getContext();
        const newChatId = contextAfterNewChat.chatId;
        console.log('[Chat Auto Backup] Step 3 - Context after new chat creation:', { // <-- Logging state after new chat
            groupId: contextAfterNewChat.groupId,
            characterId: contextAfterNewChat.characterId,
            chatId: newChatId,
            originalChatId: originalChatIdBeforeNewChat
        });


        if (!newChatId || newChatId === originalChatIdBeforeNewChat) {
            console.error('[Chat Auto Backup] Step 3 Failed: Could not obtain a valid new chatId. New ChatID:', newChatId, "Old ChatID:", originalChatIdBeforeNewChat);
            toastr.error('Failed to obtain the ID for the new chat, cannot proceed with restoration');
            return false;
        }
        console.log(`[Chat Auto Backup] Step 3: New Chat ID: ${newChatId}`); // <-- Added log

        // --- Step 4: Prepare Chat Content and Metadata ---
        console.log('[Chat Auto Backup] Step 4: Preparing chat content and metadata in memory...'); // <-- Added log
        const chatToSave = structuredClone(backupData.chat);
        // let metadataToSave = {}; // Old, would lose table metadata
        let metadataToSave = structuredClone(backupData.metadata || {}); // <-- Ensure metadata is restored
        console.log('[Chat Auto Backup] Step 4 - Chat messages to save (first two):', chatToSave.slice(0, 2)); // <-- Logging partial chat content
        console.log('[Chat Auto Backup] Step 4 - Metadata to save:', metadataToSave); // <-- Logging metadata

        // Specific check for table-related metadata
        if (metadataToSave.sheets) {
            console.log('[Chat Auto Backup] Step 4 - Restored metadata contains table definitions (sheets):', metadataToSave.sheets);
        } else {
            console.warn('[Chat Auto Backup] Step 4 - Warning: Restored metadata does NOT contain table definitions (sheets)! This may cause tables to not be restored. Metadata at time of backup:', backupData.metadata);
        }


        console.log(`[Chat Auto Backup] Step 4: Preparation complete, message count: ${chatToSave.length}, metadata:`, metadataToSave); // <-- Added log

        // --- Step 5: Save Restored Data to New Chat File ---
        console.log(`[Chat Auto Backup] Step 5: Temporarily replacing global chat and metadata for saving...`); // <-- Added log
        let globalContext = getContext();
        let originalGlobalChat = globalContext.chat.slice();
        let originalGlobalMetadata = structuredClone(globalContext.chat_metadata); // Ensure chat_metadata is backed up
        console.log('[Chat Auto Backup] Step 5 - Global chat_metadata before saving:', originalGlobalMetadata);


        globalContext.chat.length = 0;
        chatToSave.forEach(msg => globalContext.chat.push(msg));
        // updateChatMetadata(metadataToSave, true); // Old, may not be perfectly correct
        globalContext.chat_metadata = metadataToSave; // <-- Directly assign restored metadata
        console.log('[Chat Auto Backup] Step 5 - Global chat_metadata has been replaced with restored metadata:', globalContext.chat_metadata);


        console.log(`[Chat Auto Backup] Step 5: About to call saveChat({ chatName: ${newChatId}, force: true }) to save restored data...`); // <-- Added log
        try {
            await saveChat({ chatName: newChatId, force: true });
            console.log('[Chat Auto Backup] Step 5: saveChat call completed'); // <-- Added log
        } catch (saveError) {
            console.error("[Chat Auto Backup] Step 5 Failed: Error calling saveChat:", saveError);
            toastr.error(`Failed to save restored chat: ${saveError.message}`, 'Chat Auto Backup');
            // Restore state
            globalContext.chat.length = 0;
            originalGlobalChat.forEach(msg => globalContext.chat.push(msg));
            // updateChatMetadata(originalGlobalMetadata, true);
            globalContext.chat_metadata = originalGlobalMetadata; // Restore
            console.warn('[Chat Auto Backup] Step 5 - Global state restored after saveChat failure.');
            return false;
        } finally {
             // Restore state
             globalContext.chat.length = 0;
             originalGlobalChat.forEach(msg => globalContext.chat.push(msg));
             // updateChatMetadata(originalGlobalMetadata, true);
             globalContext.chat_metadata = originalGlobalMetadata; // Restore
             console.log('[Chat Auto Backup] Step 5: Global chat and chat_metadata restored to pre-save state'); // <-- Added log
        }

        // --- Step 6: Force Reload - By Closing and Reopening ---
        console.log('[Chat Auto Backup] Step 6: Initiating force reload process (close and reopen)...'); // <-- Added log
        try {
            // 6a: Trigger chat closure
            console.log("[Chat Auto Backup] Step 6a: Triggering 'Close Chat' (simulating click on #option_close_chat)");
            const closeButton = document.getElementById('option_close_chat');
            if (closeButton) {
                closeButton.click();
            } else {
                console.warn("[Chat Auto Backup] Could not find #option_close_chat button to trigger close");
            }
            await new Promise(resolve => setTimeout(resolve, 800));

            // 6b: Trigger re-selection of target entity
            console.log(`[Chat Auto Backup] Step 6b: Re-selecting target ${entityToRestore.isGroup ? 'group' : 'character'} ID: ${entityToRestore.id}`);
            if (entityToRestore.isGroup) {
                await select_group_chats(entityToRestore.id);
            } else {
                await selectCharacterById(entityToRestore.charIndex, { switchMenu: false });
            }
            await new Promise(resolve => setTimeout(resolve, 1000));

            console.log('[Chat Auto Backup] Step 6: Close and reopen process complete, UI should now be loaded correctly'); // <-- Added log
            console.log('[Chat Auto Backup] Step 6 - Context after close and reopen:', { // <-- Logging final state
                groupId: getContext().groupId,
                characterId: getContext().characterId,
                chatId: getContext().chatId,
                chatMetadata: getContext().chatMetadata // Very important! Check this
            });
            // Check if chatMetadata.sheets at this point is what you expect to restore from the backup
            if (getContext().chatMetadata && getContext().chatMetadata.sheets) {
                console.log('[Chat Auto Backup] Step 6 - After restore, current chat\'s table definitions (sheets):', getContext().chatMetadata.sheets);
            } else {
                console.warn('[Chat Auto Backup] Step 6 - Warning: After restore, current chat\'s table definitions (sheets) not found or empty!');
            }


        } catch (reloadError) {
            console.error('[Chat Auto Backup] Step 6 Failed: Error during chat closure or reopening:', reloadError);
            toastr.error('Failed to reload restored chat, please try switching chats manually. Data has been saved.');
        }

        // --- Step 7: Trigger Event ---
        console.log('[Chat Auto Backup] Step 7: Triggering CHAT_CHANGED event...'); // <-- Added log
        const finalContext = getContext();
        eventSource.emit(event_types.CHAT_CHANGED, finalContext.chatId);

        // --- End ---
        console.log('[Chat Auto Backup] Restoration process complete'); // <-- Added log
        return true;

    } catch (error) {
        console.error('[Chat Auto Backup] Unexpected critical error during chat restoration:', error);
        toastr.error(`Restoration failed: ${error.message || 'Unknown error'}`, 'Chat Auto Backup');
        return false;
    }
}

// --- UI Update ---
async function updateBackupsList() {
    console.log('[Chat Auto Backup] Starting backup list UI update');
    const backupsContainer = $('#chat_backup_list');
    if (!backupsContainer.length) {
        console.warn('[Chat Auto Backup] Could not find backup list container element #chat_backup_list');
        return;
    }

    backupsContainer.html('<div class="backup_empty_notice">Loading backups...</div>');

    try {
        const allBackups = await getAllBackups();
        backupsContainer.empty(); // Clear existing content

        if (allBackups.length === 0) {
            backupsContainer.append('<div class="backup_empty_notice">No backups saved yet</div>');
            return;
        }

        // Sort by timestamp in descending order
        allBackups.sort((a, b) => b.timestamp - a.timestamp);
        logDebug(`Rendering ${allBackups.length} backups`);

        allBackups.forEach(backup => {
            const date = new Date(backup.timestamp);
            // Use a more reliable and localized format
            const formattedDate = date.toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'medium' });

            const backupItem = $(`
                <div class="backup_item">
                    <div class="backup_info">
                        <div class="backup_header">
                            <span class="backup_entity" title="${backup.entityName}">${backup.entityName || 'Unknown Entity'}</span>
                            <span class="backup_chat" title="${backup.chatName}">${backup.chatName || 'Unknown Chat'}</span>
                        </div>
                         <div class="backup_details">
                            <span class="backup_mesid">Messages: ${backup.lastMessageId + 1}</span>
                            <span class="backup_date">${formattedDate}</span>
                        </div>
                        <div class="backup_preview" title="${backup.lastMessagePreview}">${backup.lastMessagePreview}...</div>
                    </div>
                    <div class="backup_actions">
                        <button class="menu_button backup_preview_btn" title="Preview last two messages of this backup" data-timestamp="${backup.timestamp}" data-key="${backup.chatKey}">Preview</button>
                        <button class="menu_button backup_restore" title="Restore this backup to a new chat" data-timestamp="${backup.timestamp}" data-key="${backup.chatKey}">Restore</button>
                        <button class="menu_button danger_button backup_delete" title="Delete this backup" data-timestamp="${backup.timestamp}" data-key="${backup.chatKey}">Delete</button>
                    </div>
                </div>
            `);
            backupsContainer.append(backupItem);
        });

        console.log('[Chat Auto Backup] Backup list rendering complete');
    } catch (error) {
        console.error('[Chat Auto Backup] Failed to update backup list:', error);
        backupsContainer.html(`<div class="backup_empty_notice">Failed to load backup list: ${error.message}</div>`);
    }
}

// --- Initialization and Event Binding ---
jQuery(async () => {
    console.log('[Chat Auto Backup] Plugin loading...');

    // Initialize settings
    const settings = initSettings();

    try {
        // Initialize database
        await initDatabase();

        // --- Create Web Worker ---
        try {
             // Define Worker's internal code
            const workerCode = `
                // Worker Scope
                ${deepCopyLogicString} // Inject deep copy function

                self.onmessage = function(e) {
                    const { id, payload } = e.data;
                    // console.log('[Worker] Received message with ID:', id);
                    if (!payload) {
                         // console.error('[Worker] Invalid payload received');
                         self.postMessage({ id, error: 'Invalid payload received by worker' });
                         return;
                    }
                    try {
                        const copiedChat = payload.chat ? deepCopy(payload.chat) : null;
                        const copiedMetadata = payload.metadata ? deepCopy(payload.metadata) : null;
                        // console.log('[Worker] Deep copy successful for ID:', id);
                        self.postMessage({ id, result: { chat: copiedChat, metadata: copiedMetadata } });
                    } catch (error) {
                        // console.error('[Worker] Error during deep copy for ID:', id, error);
                        self.postMessage({ id, error: error.message || 'Worker deep copy failed' });
                    }
                };
            `;
            const blob = new Blob([workerCode], { type: 'application/javascript' });
            backupWorker = new Worker(URL.createObjectURL(blob));
            console.log('[Chat Auto Backup] Web Worker created');

            // Set up Worker message handler (main thread)
            backupWorker.onmessage = function(e) {
                const { id, result, error } = e.data;
                // logDebug(`[Main Thread] Received message from Worker (ID: ${id})`);
                if (workerPromises[id]) {
                    if (error) {
                        console.error(`[Main Thread] Worker returned an error (ID: ${id}):`, error);
                        workerPromises[id].reject(new Error(error));
                    } else {
                        // logDebug(`[Main Thread] Worker returned result (ID: ${id})`);
                        workerPromises[id].resolve(result);
                    }
                    delete workerPromises[id]; // Clean up Promise record
                } else {
                     console.warn(`[Main Thread] Received message for unknown or already processed Worker ID (ID: ${id})`);
                }
            };

            // Set up Worker error handler (main thread)
            backupWorker.onerror = function(error) {
                console.error('[Chat Auto Backup] Web Worker encountered an error:', error);
                 // Reject any pending promises
                 Object.keys(workerPromises).forEach(id => {
                     workerPromises[id].reject(new Error('Worker encountered an unrecoverable error.'));
                     delete workerPromises[id];
                 });
                toastr.error('Backup Worker encountered an error, auto backup may have stopped', 'Chat Auto Backup');
                 // Consider attempting to recreate the Worker here
            };

        } catch (workerError) {
            console.error('[Chat Auto Backup] Failed to create Web Worker:', workerError);
            backupWorker = null; // Ensure worker instance is null
            toastr.error('Failed to create backup Worker, will fallback to main thread backup (lower performance)', 'Chat Auto Backup');
            // In this case, performDeepCopyInWorker needs a fallback mechanism (or the plugin should error/disable)
            // For now, simplifying: if Worker creation fails, backup functionality will error
        }

        // Load plugin UI
        const settingsHtml = await renderExtensionTemplateAsync(
            `third-party/${PLUGIN_NAME}`,
            'settings'
        );
        $('#extensions_settings').append(settingsHtml);
        console.log('[Chat Auto Backup] Settings interface added');

        // Settings controls
        const $settingsBlock = $('<div class="chat_backup_control_item"></div>');
        $settingsBlock.html(`
            <div style="margin-bottom: 8px;">
                <label style="display: inline-block; min-width: 120px;">Debounce Delay (ms):</label>
                <input type="number" id="chat_backup_debounce_delay" value="${settings.backupDebounceDelay}" 
                    min="300" max="10000" step="100" title="Wait how many milliseconds after editing or deleting a message before performing backup (recommend 1000-1500)" 
                    style="width: 80px;" />
            </div>
            <div>
                <label style="display: inline-block; min-width: 120px;">System Max Backups:</label>
                <input type="number" id="chat_backup_max_total" value="${settings.maxTotalBackups}" 
                    min="1" max="10" step="1" title="Maximum number of backups to retain in the system" 
                    style="width: 80px;" />
            </div>
        `);
        $('.chat_backup_controls').prepend($settingsBlock);
        
        // Add listener for max backups setting
        $(document).on('input', '#chat_backup_max_total', function() {
            const total = parseInt($(this).val(), 10);
            if (!isNaN(total) && total >= 1 && total <= 10) {
                settings.maxTotalBackups = total;
                logDebug(`System max backups updated to: ${total}`);
                saveSettingsDebounced();
            } else {
                logDebug(`Invalid input for system max backups: ${$(this).val()}`);
                $(this).val(settings.maxTotalBackups);
            }
        });

        // --- Bind UI events using event delegation ---
        $(document).on('click', '#chat_backup_manual_backup', performManualBackup);

        // Debounce delay setting
        $(document).on('input', '#chat_backup_debounce_delay', function() {
            const delay = parseInt($(this).val(), 10);
            if (!isNaN(delay) && delay >= 300 && delay <= 10000) {
                settings.backupDebounceDelay = delay;
                logDebug(`Debounce delay updated to: ${delay}ms`);
                saveSettingsDebounced();
            } else {
                logDebug(`Invalid input for debounce delay: ${$(this).val()}`);
                $(this).val(settings.backupDebounceDelay);
            }
        });

        // Restore button
        $(document).on('click', '.backup_restore', async function() {
            const button = $(this);
            const timestamp = parseInt(button.data('timestamp'));
            const chatKey = button.data('key');
            logDebug(`Clicked restore button, timestamp: ${timestamp}, chatKey: ${chatKey}`);

            button.prop('disabled', true).text('Restoring...'); // Disable button and show status

            try {
                const db = await getDB();
                const backup = await new Promise((resolve, reject) => {
                    const transaction = db.transaction([STORE_NAME], 'readonly');
                    
                    transaction.onerror = (event) => {
                        reject(event.target.error);
                    };
                    
                    const store = transaction.objectStore(STORE_NAME);
                    const request = store.get([chatKey, timestamp]);
                    
                    request.onsuccess = () => {
                        resolve(request.result);
                    };
                    
                    request.onerror = (event) => {
                        reject(event.target.error);
                    };
                });

                if (backup) {
                    if (confirm(`Are you sure you want to restore the backup for "${backup.entityName} - ${backup.chatName}"?\n\nThis will select the corresponding character/group and create a [NEW CHAT] to restore the backup content.\n\nYour current chat content will not be lost, but please ensure it is saved.`)) {
                        const success = await restoreBackup(backup);
                        if (success) {
                            toastr.success('Chat history successfully restored to a new chat');
                        }
                    }
                } else {
                    console.error('[Chat Auto Backup] Could not find the specified backup:', { timestamp, chatKey });
                    toastr.error('Could not find the specified backup');
                }
            } catch (error) {
                console.error('[Chat Auto Backup] Error during restoration:', error);
                toastr.error(`Error during restoration: ${error.message}`);
            } finally {
                button.prop('disabled', false).text('Restore'); // Restore button state
            }
        });

        // Delete button
        $(document).on('click', '.backup_delete', async function() {
            const button = $(this);
            const timestamp = parseInt(button.data('timestamp'));
            const chatKey = button.data('key');
            logDebug(`Clicked delete button, timestamp: ${timestamp}, chatKey: ${chatKey}`);

            const backupItem = button.closest('.backup_item');
            const entityName = backupItem.find('.backup_entity').text();
            const chatName = backupItem.find('.backup_chat').text();
            const date = backupItem.find('.backup_date').text();

            if (confirm(`Are you sure you want to permanently delete this backup?\n\nEntity: ${entityName}\nChat: ${chatName}\nDate: ${date}\n\nThis action cannot be undone!`)) {
                button.prop('disabled', true).text('Deleting...');
                try {
                    await deleteBackup(chatKey, timestamp);
                    toastr.success('Backup deleted');
                    backupItem.fadeOut(300, function() { $(this).remove(); }); // Smoothly remove the item
                    // Optional: If the list becomes empty, show a message
                    if ($('#chat_backup_list .backup_item').length <= 1) { // <=1 because the current one is still in DOM before removal
                        updateBackupsList(); // Reload to show the "no backups" message
                    }
                } catch (error) {
                    console.error('[Chat Auto Backup] Failed to delete backup:', error);
                    toastr.error(`Failed to delete backup: ${error.message}`);
                    button.prop('disabled', false).text('Delete');
                }
            }
        });

        // Preview button
        $(document).on('click', '.backup_preview_btn', async function() {
            const button = $(this);
            const timestamp = parseInt(button.data('timestamp'));
            const chatKey = button.data('key');
            logDebug(`Clicked preview button, timestamp: ${timestamp}, chatKey: ${chatKey}`);

            button.prop('disabled', true).text('Loading...'); // Disable button and show status

            try {
                const db = await getDB();
                const backup = await new Promise((resolve, reject) => {
                    const transaction = db.transaction([STORE_NAME], 'readonly');
                    
                    transaction.onerror = (event) => {
                        reject(event.target.error);
                    };
                    
                    const store = transaction.objectStore(STORE_NAME);
                    const request = store.get([chatKey, timestamp]);
                    
                    request.onsuccess = () => {
                        resolve(request.result);
                    };
                    
                    request.onerror = (event) => {
                        reject(event.target.error);
                    };
                });

                if (backup && backup.chat && backup.chat.length > 0) {
                    // Get the last two messages
                    const chat = backup.chat;
                    const lastMessages = chat.slice(-2);
                    
                    // Filter out tags and process Markdown
                    const processMessage = (messageText) => {
                        if (!messageText) return '(Empty Message)';
                        
                        // Filter out <think> and <thinking> tags and their content
                        let processed = messageText
                            .replace(/<think>[\s\S]*?<\/think>/g, '')
                            .replace(/<thinking>[\s\S]*?<\/thinking>/g, '');
                        
                        // Filter out code blocks and specific names
                        processed = processed
                            .replace(/```[\s\S]*?```/g, '')    // Remove code blocks
                            .replace(/`[\s\S]*?`/g, '');       // Remove inline code
                        
                        // Basic Markdown processing, preserving some formatting
                        processed = processed
                            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')  // Bold
                            .replace(/\*(.*?)\*/g, '<em>$1</em>')              // Italic
                            .replace(/\n\n+/g, '\n')                         // Replace multiple consecutive newlines with one
                            .replace(/\n/g, '<br>');                           // Newlines to <br>
                        
                        return processed;
                    };
                    
                    // Create styling
                    const style = document.createElement('style');
                    style.textContent = `
                        .message_box {
                            padding: 10px;
                            margin-bottom: 10px;
                            border-radius: 8px;
                            background: rgba(0, 0, 0, 0.15);
                        }
                        .message_sender {
                            font-weight: bold;
                            margin-bottom: 5px;
                            color: var(--SmColor);
                        }
                        .message_content {
                            white-space: pre-wrap;
                            line-height: 1.4;
                        }
                        .message_content br + br {
                            margin-top: 0.5em;
                        }
                    `;
                    
                    // Create preview content
                    const previewContent = document.createElement('div');
                    previewContent.appendChild(style);
                    
                    const headerDiv = document.createElement('h3');
                    headerDiv.textContent = `${backup.entityName} - ${backup.chatName} Preview`;
                    previewContent.appendChild(headerDiv);
                    
                    const contentDiv = document.createElement('div');
                    
                    // Create separate boxes for each message
                    lastMessages.forEach(msg => {
                        const messageBox = document.createElement('div');
                        messageBox.className = 'message_box';
                        
                        const senderDiv = document.createElement('div');
                        senderDiv.className = 'message_sender';
                        senderDiv.textContent = msg.name || 'Unknown';
                        
                        const contentDiv = document.createElement('div');
                        contentDiv.className = 'message_content';
                        contentDiv.innerHTML = processMessage(msg.mes);
                        
                        messageBox.appendChild(senderDiv);
                        messageBox.appendChild(contentDiv);
                        
                        previewContent.appendChild(messageBox);
                    });
                    
                    const footerDiv = document.createElement('div');
                    footerDiv.style.marginTop = '10px';
                    footerDiv.style.opacity = '0.7';
                    footerDiv.style.fontSize = '0.9em';
                    footerDiv.textContent = `Displaying last ${lastMessages.length} messages out of ${chat.length}`;
                    previewContent.appendChild(footerDiv);
                    
                    // Import dialog system
                    const { callGenericPopup, POPUP_TYPE } = await import('../../../popup.js');
                    
                    // Display preview content using system popup
                    await callGenericPopup(previewContent, POPUP_TYPE.DISPLAY, '', {
                        wide: true,
                        allowVerticalScrolling: true,
                        leftAlign: true,
                        okButton: 'Close'
                    });
                    
                } else {
                    console.error('[Chat Auto Backup] Could not find the specified backup or backup is empty:', { timestamp, chatKey });
                    toastr.error('Could not find the specified backup or backup is empty');
                }
            } catch (error) {
                console.error('[Chat Auto Backup] Error during preview:', error);
                toastr.error(`Error during preview: ${error.message}`);
            } finally {
                button.prop('disabled', false).text('Preview'); // Restore button state
            }
        });

        // Debug toggle
        $(document).on('change', '#chat_backup_debug_toggle', function() {
            settings.debug = $(this).prop('checked');
            console.log('[Chat Auto Backup] Debug mode has been ' + (settings.debug ? 'enabled' : 'disabled'));
            saveSettingsDebounced();
        });

        // Initialize UI state (delayed to ensure DOM is rendered)
        setTimeout(async () => {
            $('#chat_backup_debug_toggle').prop('checked', settings.debug);
            $('#chat_backup_debounce_delay').val(settings.backupDebounceDelay);
            $('#chat_backup_max_total').val(settings.maxTotalBackups);
            await updateBackupsList();
        }, 300);

        // --- Set up optimized event listeners ---
        function setupBackupEvents() {
            // Events triggering immediate backup (clear indication of finished state)
            const immediateBackupEvents = [
                event_types.MESSAGE_SENT,           // After user sends a message
                event_types.GENERATION_ENDED,       // After AI generation ends and message is added
                event_types.CHARACTER_FIRST_MESSAGE_SELECTED, // When selecting the first message of a character                
            ].filter(Boolean); // Filter out any potentially undefined event types

            // Events triggering debounced backup (edit-like operations)
            const debouncedBackupEvents = [
                event_types.MESSAGE_EDITED,        // After editing a message (debounce)
                event_types.MESSAGE_DELETED,       // After deleting a message (debounce)
                event_types.MESSAGE_SWIPED,         // After user swipes AI reply (debounce)
                event_types.IMAGE_SWIPED,           // Image swipe (debounce)
                event_types.MESSAGE_FILE_EMBEDDED, // File embed (debounce)
                event_types.MESSAGE_REASONING_EDITED, // Edit reasoning (debounce)
                event_types.MESSAGE_REASONING_DELETED, // Delete reasoning (debounce)
                event_types.FILE_ATTACHMENT_DELETED, // Attachment deletion (debounce)
                event_types.GROUP_UPDATED, // Group metadata update (debounce)
            ].filter(Boolean);

            console.log('[Chat Auto Backup] Setting up immediate backup event listeners:', immediateBackupEvents);
            immediateBackupEvents.forEach(eventType => {
                if (!eventType) {
                    console.warn('[Chat Auto Backup] Detected undefined immediate backup event type');
                    return;
                }
                eventSource.on(eventType, () => {
                    logDebug(`Event triggered (immediate backup): ${eventType}`);
                    // Use the new conditional backup function
                    performBackupConditional().catch(error => {
                        console.error(`[Chat Auto Backup] Immediate backup event ${eventType} handling failed:`, error);
                    });
                });
            });

            console.log('[Chat Auto Backup] Setting up debounced backup event listeners:', debouncedBackupEvents);
            debouncedBackupEvents.forEach(eventType => {
                if (!eventType) {
                    console.warn('[Chat Auto Backup] Detected undefined debounced backup event type');
                    return;
                }
                eventSource.on(eventType, () => {
                    logDebug(`Event triggered (debounced backup): ${eventType}`);
                    // Use the new debounced backup function
                    performBackupDebounced();
                });
            });

            console.log('[Chat Auto Backup] Event listener setup complete');
        }

        setupBackupEvents(); // Apply new event binding logic

        // Listen for extension page open event to refresh list
        $(document).on('click', '#extensionsMenuButton', () => {
            if ($('#chat_auto_backup_settings').is(':visible')) {
                console.log('[Chat Auto Backup] Extensions menu button clicked, and this plugin\'s settings are visible, refreshing backup list');
                setTimeout(updateBackupsList, 200); // Add a small delay to ensure panel content is loaded
            }
        });

        // Refresh list when drawer opens as well
        $(document).on('click', '#chat_auto_backup_settings .inline-drawer-toggle', function() {
            const drawer = $(this).closest('.inline-drawer');
            // Check if the drawer is about to open (based on current 'open' class)
            if (!drawer.hasClass('open')) {
                console.log('[Chat Auto Backup] Plugin settings drawer opening, refreshing backup list');
                setTimeout(updateBackupsList, 50); // Refresh almost immediately
            }
        });

        // Initial backup check (executed with a delay to ensure chat is loaded)
        setTimeout(async () => {
            logDebug('[Chat Auto Backup] Performing initial backup check');
            const context = getContext();
            if (context.chat && context.chat.length > 0 && !isBackupInProgress) {
                logDebug('[Chat Auto Backup] Found existing chat messages, performing initial backup');
                try {
                    await performBackupConditional(); // Use the conditional function
                } catch (error) {
                    console.error('[Chat Auto Backup] Initial backup execution failed:', error);
                }
            } else {
                logDebug('[Chat Auto Backup] No active chat messages or backup in progress, skipping initial backup');
            }
        }, 4000); // Slightly longer delay to wait for application initialization

        console.log('[Chat Auto Backup] Plugin loading complete');

    } catch (error) {
        console.error('[Chat Auto Backup] Critical error during plugin loading:', error);
        // Display error message in the UI
        $('#extensions_settings').append(
            '<div class="error">Chat Auto Backup plugin failed to load. Please check the console.</div>'
        );
    }
});
