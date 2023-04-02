import sys
import time
import uuid
from PyQt5.QtWidgets import QWidget, QApplication, QStackedLayout, QMessageBox
from PyQt5.uic import loadUi
import hashlib
from multiprocessing.managers import BaseManager
import multiprocessing as mp

# constant values for address, port and key
SERVER_ADDRESS = 'localhost'
SERVER_PORT = 50000
AUTH_KEY = b'your_secret_key'

# internal queue used to manage new messages
internal_queue = mp.Queue()
internal_queue.put([])


# creates a popup describing the error
class ErrorPopup(QMessageBox):
    def __init__(self, message):
        super().__init__()
        self.setIcon(QMessageBox.Critical)
        self.setText("Error")
        self.setInformativeText(message)
        self.setWindowTitle("Error")
        self.exec_()


class QueueManager(BaseManager):
    pass


class Login(QWidget):
    def __init__(self, lout):
        super(Login, self).__init__()
        loadUi('login.ui', self)
        self.login_button.clicked.connect(self.login)
        self.load_register.clicked.connect(self.goto_register)
        self.layout = lout

    def login(self):
        # gets login details
        usr = self.username.text()
        passwd = hashlib.md5(self.password.text().encode()).hexdigest()
        # sends them to server via message queue
        messages = self.layout.queue.get()
        messages.append(f'SERVER {self.layout.uuid} LOGIN {usr} {passwd}')
        self.layout.queue.put(messages)
        # wait for confirmation from the server
        try:
            self.layout.handle_message()
            self.layout.self_username = usr
            self.layout.setCurrentIndex(2)
        except Exception as exc:
            ErrorPopup(str(exc))

    def goto_register(self):
        self.layout.setCurrentIndex(1)


class Register(QWidget):

    def __init__(self, lout):
        super(Register, self).__init__()
        loadUi('register.ui', self)
        self.register_button.clicked.connect(self.register)
        self.load_login.clicked.connect(self.goto_login)
        self.layout = lout

    def register(self):
        # gets login details
        usr = self.username.text()
        passwd = hashlib.md5(self.password.text().encode()).hexdigest()
        # sends them to server
        messages = self.layout.queue.get()
        messages.append(f'SERVER {self.layout.uuid} REGISTER {usr} {passwd}')
        self.layout.queue.put(messages)
        # waits for confirmation from server
        try:
            self.layout.handle_message()
            print("Success")
            self.layout.self_username = usr
            self.layout.setCurrentIndex(2)
        except Exception as exc:
            ErrorPopup(str(exc))

    def goto_login(self):
        self.layout.setCurrentIndex(0)


class Chat(QWidget):
    def __init__(self, lout):
        super(Chat, self).__init__()
        loadUi('chat.ui', self)
        self.search_user.clicked.connect(self.get_user)
        self.send_message.clicked.connect(self.send)
        self.refresh_button.clicked.connect(self.refresh)
        self.layout = lout

    def get_user(self):
        # gets username
        usr = self.receiver_username.text()
        # asks server for messages
        messages = self.layout.queue.get()
        messages.append(f'SERVER {self.layout.uuid} GET_MESSAGES {usr}')
        self.layout.queue.put(messages)
        # handles message stream from server
        try:
            chat_messages = self.layout.handle_message()
            # checks if the chat updater is active and kills it if so
            if self.layout.chat_updater.is_alive():
                self.layout.chat_updater.kill()
            # adds the messages to the chat window
            self.chat_window.setPlainText('')
            for message in chat_messages:
                self.chat_window.append(message)
            # sets the user as it's target for checking new messages
            self.layout.target_username = usr
            # empties the internal queue
            internal_queue.get()
            internal_queue.put([])
            # starts a new chat updater
            self.layout.chat_updater = ChatUpdater(self.layout)
            self.layout.chat_updater.start()
        except Exception as exc:
            ErrorPopup(str(exc))

    def send(self):
        # gets target user
        usr = self.layout.target_username
        # if no user is targeted, shows a popup
        if usr is None:
            ErrorPopup("NO_TARGET")
            return
        # gets the message
        msg = f'{self.layout.self_username}: {self.chat_message.text()}'
        # updates the chat window locally
        self.chat_window.append(msg)
        # sends the message to the server
        messages = self.layout.queue.get()
        messages.append(f'SERVER {self.layout.uuid} SEND_MESSAGE {usr} {msg}')
        self.layout.queue.put(messages)

        print("Success")

    def refresh(self):
        # gets all new messages from the internal queue
        messages = internal_queue.get()
        for message in messages:
            self.chat_window.append(message)
        internal_queue.put([])

    def closeEvent(self, event):
        # signals server that the cleint is no longer connected
        messages = self.layout.queue.get()
        messages.append(f'SERVER {self.layout.uuid} SHUTDOWN')
        self.layout.queue.put(messages)
        self.layout.chat_updater.kill()


class Layout(QStackedLayout):

    def __init__(self):
        # connects to the message queue
        self.uuid = str(uuid.uuid4())
        QueueManager.register('get_queue')
        manager = QueueManager(address=(SERVER_ADDRESS, SERVER_PORT), authkey=AUTH_KEY)
        manager.connect()
        # initializes attributes
        self.queue = manager.get_queue()
        self.self_username = None
        self.target_username = None
        self.chat_updater = ChatUpdater(self)
        self.chat_updater.start()
        super(Layout, self).__init__()
        # adds the widgets to the layout
        self.addWidget(Login(self))
        self.addWidget(Register(self))
        self.addWidget(Chat(self))

    def handle_message(self):
        # PROTOCOL: TARGET(CLIENT/SERVER) CLIENT_UUID COMMAND ARGS
        while True:
            messages = self.queue.get()
            # checks if there are any messages
            if len(messages) > 0:
                print(f'received: {messages[0]}')
                message = messages[0].split(' ', maxsplit=3)
                # checks if the message is addressed to this instance
                if (message[0] != 'CLIENT') or (message[1] != self.uuid):
                    self.queue.put(messages)
                    time.sleep(1)
                    continue
                # extracts message and puts the queue back
                messages.pop(0)
                self.queue.put(messages)
                # message is meant for this client, handles it
                match message[2]:
                    case 'CONNECTED':
                        break
                    case 'FAILED':
                        raise Exception(message[3])
                    case 'MESSAGE':
                        # gets all messages recursively
                        remaining, msg = message[3].split(' ', maxsplit=1)
                        ret = [msg]
                        if remaining == '-1':
                            return []
                        if remaining != '0':
                            ret += self.handle_message()
                        return ret
            else:
                self.queue.put(messages)
            time.sleep(1)


class ChatUpdater(mp.Process):
    def __init__(self, client_layout):
        # gets the layout reference passed as argument
        self.client_layout = client_layout
        super(ChatUpdater, self).__init__()

    def run(self):
        while True:
            time.sleep(1)
            messages = self.client_layout.queue.get()
            # checks if there are any messages
            if len(messages) > 0:
                print(f'chat updater received: {messages[0]}')
                message = messages[0].split(' ', maxsplit=3)
                # checks if message is meant for this client
                if (message[0] != 'CLIENT') or (message[1] != self.client_layout.uuid) or (message[2] != 'NEW_MESSAGE'):
                    self.client_layout.queue.put(messages)
                    continue
                # message is meant for us, extract it and return queue
                messages.pop(0)
                self.client_layout.queue.put(messages)
                sender_username, msg = message[3].split(' ', maxsplit=1)
                # checks if the sender is the active chat
                if sender_username != self.client_layout.target_username:
                    continue
                # puts the message in the internal queue
                new_messages = internal_queue.get()
                new_messages.append(f'{msg}\n')
                internal_queue.put(new_messages)
            else:
                self.client_layout.queue.put(messages)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    layout = Layout()
    sys.exit(app.exec_())
