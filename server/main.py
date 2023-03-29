import multiprocessing as mp
import time
from multiprocessing.managers import BaseManager
import os
import sqlite3
from collections import namedtuple

# constant values for address, port and key
SERVER_ADDRESS = 'localhost'
SERVER_PORT = 50000
AUTH_KEY = b'your_secret_key'

# Create tuples to represent db objects
User = namedtuple("User", ["id", "username", "password"])
Message = namedtuple("Message", ["id", "sender_id", "receiver_id", "message"])


# Exceptions to handle database/logging errors
class IncorrectPasswordException(Exception):
    pass


class UserNotFoundException(Exception):
    pass


class QueueManager(BaseManager):
    pass


# Creates a server that hosts the message queue
class QueueServer(mp.Process):
    def __init__(self, message_queue):
        self.queue = message_queue
        self.queue.put([])
        super(QueueServer, self).__init__()

    def run(self):
        print("Server started.")
        QueueManager.register('get_queue', callable=lambda: self.queue)
        manager = QueueManager(address=(SERVER_ADDRESS, SERVER_PORT), authkey=AUTH_KEY)
        sv = manager.get_server()
        sv.serve_forever()


# Handles database interaction
class DatabaseManager:
    CREATE_USERS_CMD = '''CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password TEXT NOT NULL)'''
    CREATE_MESSAGES_CMD = '''CREATE TABLE IF NOT EXISTS messages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sender_id INTEGER NOT NULL,
                        receiver_id INTEGER NOT NULL,
                        message TEXT NOT NULL)'''
    INSERT_USER_CMD = '''INSERT INTO users(username, password)
                    VALUES (?, ?)'''
    INSERT_MESSAGE_CMD = '''INSERT INTO messages(sender_id, receiver_id, message)
                        VALUES (?, ?, ?)'''
    SELECT_USER_BY_USERNAME_CMD = '''SELECT * FROM users WHERE username=?'''
    SELECT_USER_BY_ID_CMD = '''SELECT * FROM users WHERE id=?'''
    SELECT_MESSAGES_BY_USERS_ID_CMD = '''SELECT * FROM messages WHERE (sender_id = ? AND receiver_id = ?) 
                                    OR (sender_id = ? AND receiver_id = ?)'''

    CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
    DATABASE_PATH = os.path.join(CURRENT_PATH, 'chat_db.db')

    def __init__(self):
        # Creates the tables for users and messages
        with sqlite3.connect(self.DATABASE_PATH) as db:
            cursor = db.cursor()
            cursor.execute(self.CREATE_USERS_CMD)
            cursor.execute(self.CREATE_MESSAGES_CMD)
            cursor.close()

    def get_user_by_username(self, username):
        # returns the user object if successful
        # if user not found, throws exception
        with sqlite3.connect(self.DATABASE_PATH) as db:
            cursor = db.cursor()
            cursor.execute(self.SELECT_USER_BY_USERNAME_CMD, (username,))
            rows = cursor.fetchall()
            cursor.close()

        if len(rows) != 0:
            return User(*rows[0])
        else:
            raise UserNotFoundException("NOT_FOUND")

    def get_user_by_id(self, username):
        # returns the user object if successful
        # if user not found, throws exception
        with sqlite3.connect(self.DATABASE_PATH) as db:
            cursor = db.cursor()
            cursor.execute(self.SELECT_USER_BY_ID_CMD, (username,))
            rows = cursor.fetchall()
            cursor.close()

        if len(rows) != 0:
            return User(*rows[0])
        else:
            raise UserNotFoundException("NOT_FOUND")

    def register(self, args):
        # Inserts the new user into the db, then extracts it to get the ID and return it
        # raises an exception if username was already used
        with sqlite3.connect(self.DATABASE_PATH) as db:
            cursor = db.cursor()
            username, password = args.split(' ')
            cursor.execute(self.INSERT_USER_CMD, (username, password))
            cursor.close()
        return self.get_user_by_username(username).id

    def login(self, args):
        # Gets the user by its username, then checks if the password hashes are the same
        # Returns the ID if successful, raises exception otherwise
        username, password = args.split(' ')
        user = self.get_user_by_username(username)
        if user.password == password:
            return user.id
        else:
            raise IncorrectPasswordException("WRONG_PASSWORD")

    def get_messages(self, requester_id, args):
        # gets messages and returns them as a list
        user_id = self.get_user_by_username(args).id
        with sqlite3.connect(self.DATABASE_PATH) as db:
            cursor = db.cursor()
            cursor.execute(self.SELECT_MESSAGES_BY_USERS_ID_CMD, (requester_id, user_id, user_id, requester_id))
            rows = cursor.fetchall()
            cursor.close()
        return [Message(*row) for row in rows]

    def send_message(self, requester_id, args):
        # saves the message into the db
        username, message = args.split(' ', maxsplit=1)
        user_id = self.get_user_by_username(username).id
        with sqlite3.connect(self.DATABASE_PATH) as db:
            cursor = db.cursor()
            cursor.execute(self.INSERT_MESSAGE_CMD, (requester_id, user_id, message))
            cursor.close()
        return user_id


class MessageHandler(mp.Process):
    # Handles the messages
    def __init__(self, message_queue):
        self.message_queue = message_queue
        self.db_manager = DatabaseManager()
        super(MessageHandler, self).__init__()

    def run(self):
        # PROTOCOL: TARGET(CLIENT/SERVER) CLIENT_UUID COMMAND ARGS
        print("Handler started.")
        users = dict()
        while True:
            messages = self.message_queue.get()
            # catches database/logging exceptions + empty queue
            try:
                print(f'received: {messages[0]}')
                message = messages[0].split(' ', maxsplit=3)
                # checks if message is meant for the server
                if message[0] != 'SERVER':
                    self.message_queue.put(messages)
                    time.sleep(1)
                    continue
                # message is meant for us, extract it
                messages.pop(0)
                match message[2]:
                    case 'LOGIN':
                        # attempts to login through the database manager, may raise exception
                        user_id = self.db_manager.login(message[3])
                        # login successful
                        users[message[1]] = user_id
                        messages.append(f'CLIENT {message[1]} CONNECTED')

                    case 'REGISTER':
                        # attempts to register through the database manager, may raise exception
                        user_id = self.db_manager.register(message[3])
                        # register successful
                        users[message[1]] = user_id
                        messages.append(f'CLIENT {message[1]} CONNECTED')
                    case 'GET_MESSAGES':
                        # gets the messages from the database, then sends them one by one
                        user_id = users[message[1]]
                        chat_messages = self.db_manager.get_messages(user_id, message[3])
                        index = len(chat_messages) - 1
                        if index == -1:
                            messages.append(f'CLIENT {message[1]} MESSAGE {index} EMPTY')
                        for msg in chat_messages:
                            messages.append(f'CLIENT {message[1]} MESSAGE {index} {msg.message}')
                            index -= 1

                    case 'SEND_MESSAGE':
                        # saves the message into the database
                        user_id = users[message[1]]
                        # if user doesnt exists, raises exception
                        user_name = self.db_manager.get_user_by_id(user_id).username
                        receiver_id = self.db_manager.send_message(user_id, message[3])
                        receiver_uuid = ""
                        msg = message[3].split(' ', maxsplit=1)
                        # alerts the receiver if his client is open
                        if receiver_id in users.values():
                            for uuid, rec_id in users.items():
                                if rec_id == receiver_id:
                                    receiver_uuid = uuid
                                    break
                            messages.append(f'CLIENT {receiver_uuid} NEW_MESSAGE {user_name} {msg[1]}')

                    case 'SHUTDOWN':
                        # removes user from active clients dictionary
                        del users[message[1]]
            except IndexError:
                pass
            except UserNotFoundException:
                messages.append(f'CLIENT {message[1]} FAILED NOT_FOUND')
            except IncorrectPasswordException:
                messages.append(f'CLIENT {message[1]} FAILED WRONG_PASSWORD')
            except sqlite3.IntegrityError:
                messages.append(f'CLIENT {message[1]} FAILED ALREADY_EXISTS')
            finally:
                self.message_queue.put(messages)
                time.sleep(1)


if __name__ == '__main__':
    queue = mp.Queue()
    server = QueueServer(queue)
    handler = MessageHandler(queue)
    handler.start()
    server.start()


