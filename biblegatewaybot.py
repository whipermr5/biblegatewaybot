import webapp2
import logging
import json
import textwrap
import urllib
import re
from bs4 import BeautifulSoup
from scriptures import extract as extract_refs
from google.appengine.api import urlfetch, urlfetch_errors, taskqueue
from google.appengine.ext import db
from datetime import datetime, timedelta
from xml.etree import ElementTree as etree

EMPTY = 'empty'

def strip_markdown(string):
    return string.replace('*', '\*').replace('_', '\_').replace('`', '\`').replace('[', '\[')

def get_passage(passage, version='NIV'):
    def to_sup(text):
        sups = {u'0': u'\u2070',
                u'1': u'\xb9',
                u'2': u'\xb2',
                u'3': u'\xb3',
                u'4': u'\u2074',
                u'5': u'\u2075',
                u'6': u'\u2076',
                u'7': u'\u2077',
                u'8': u'\u2078',
                u'9': u'\u2079',
                u'-': u'\u207b'}
        return ''.join(sups.get(char, char) for char in text)

    BG_URL = 'https://www.biblegateway.com/passage/?search={}&version={}&interface=print'

    search = urllib.quote(passage.lower().strip())
    url = BG_URL.format(search, version)
    try:
        result = urlfetch.fetch(url, deadline=10)
    except urlfetch_errors.Error as e:
        logging.warning('Error fetching passage:\n' + str(e))
        return None
    html = result.content
    soup = BeautifulSoup(html, 'lxml').select_one('.passage-text')

    if not soup:
        return EMPTY

    WANTED = 'bg-bot-passage-text'
    UNWANTED = '.passage-display, .footnote, .footnotes, .crossrefs, .publisher-info-bottom'

    title = soup.select_one('.passage-display-bcv').text
    header = '*' + strip_markdown(title.strip()) + '* (' + version + ')'

    for tag in soup.select(UNWANTED):
        tag.decompose()

    for tag in soup.select('h1, h2, h3, h4, h5, h6'):
        tag['class'] = WANTED
        text = tag.text.strip().replace(' ', '\a')
        tag.string = '*' + strip_markdown(text) + '*'

    needed_stripping = False

    for tag in soup.select('p'):
        tag['class'] = WANTED
        bad_strings = tag(text=re.compile('(\*|\_|\`|\[)'))
        for bad_string in bad_strings:
            stripped_text = strip_markdown(unicode(bad_string))
            bad_string.replace_with(stripped_text)
            needed_stripping = True

    if needed_stripping:
        logging.info('Stripped markdown')

    for tag in soup.select('br'):
        tag.name = 'span'
        tag.string = '\n'

    for tag in soup.select('.chapternum'):
        num = tag.text.strip()
        tag.string = '*' + strip_markdown(num) + '* '

    for tag in soup.select('.versenum'):
        num = tag.text.strip()
        tag.string = to_sup(num)

    for tag in soup.select('.text'):
        tag.string = tag.text.rstrip()

    final_text = header + '\n\n'
    for tag in soup(class_=WANTED):
        final_text += tag.text.strip() + '\n\n'

    return final_text.strip()

MAX_SEARCH_RESULTS = 10

def get_search_results(text, start=0):
    BH_URL = 'http://216.58.158.10/search?q={}&output=xml_no_dtd&client=default_frontend&num=' + \
             str(MAX_SEARCH_RESULTS) + '&oe=UTF-8&ie=UTF-8&site=biblecc&filter=0&start={}'

    query = urllib.quote(text.lower().strip())
    url = BH_URL.format(query, start)
    try:
        result = urlfetch.fetch(url, deadline=10)
    except urlfetch_errors.Error as e:
        logging.warning('Error fetching search results:\n' + str(e))
        return None

    xml = result.content
    tree = etree.fromstring(xml)

    results_body = ''
    for result in tree.iterfind('RES/R'):
        header = result.find('T').text
        content = result.find('S').text

        header = BeautifulSoup(header, 'lxml').text
        idx = header.find(':')
        idx += header[idx:].find(' ')
        title = strip_markdown(header[:idx].strip())

        soup = BeautifulSoup(content, 'lxml')

        bad_strings = soup(text=re.compile('(\*|\_)'))
        for bad_string in bad_strings:
            stripped_text = strip_markdown(unicode(bad_string))
            bad_string.replace_with(stripped_text)

        for tag in soup('b'):
            if tag.text == u'...':
                continue
            tag.string = '*' + tag.text + '*'

        description = soup.text.strip()

        link = '/' + ''.join(title.split()).lower().replace(':', 'V')

        results_body += u'\U0001F539' + title + '\n' + description + '\n' + link + '\n\n'

    if not results_body:
        return EMPTY

    final_text = 'Search results'

    res = tree.find('RES')
    sn = res.get('SN')
    en = res.get('EN')
    total = res.find('M').text

    if start != int(sn) - 1:
        return EMPTY

    if int(total) > MAX_SEARCH_RESULTS:
        final_text += ' ({}-{} of {})'.format(sn, en, total)

    final_text += '\n\n' + results_body.strip()

    if int(en) < int(total):
        final_text += '\n\nGet /more results'

    return final_text

def other_version(current_version):
    if current_version == 'NASB':
        return 'NIV'
    return 'NASB'

from secrets import TOKEN, ADMIN_ID, BOT_ID, BOTFAMILY_HASH
from versions import VERSION_DATA, VERSION_LOOKUP, VERSIONS, BOOKS
TELEGRAM_URL = 'https://api.telegram.org/bot' + TOKEN
TELEGRAM_URL_SEND = TELEGRAM_URL + '/sendMessage'
TELEGRAM_URL_CHAT_ACTION = TELEGRAM_URL + '/sendChatAction'
JSON_HEADER = {'Content-Type': 'application/json;charset=utf-8'}

LOG_SENT = '{} {} sent to uid {} ({})'
LOG_ENQUEUED = 'Enqueued {} to uid {} ({})'
LOG_DID_NOT_SEND = 'Did not send {} to uid {} ({}): {}'
LOG_ERROR_SENDING = 'Error sending {} to uid {} ({}):\n{}'
LOG_ERROR_DATASTORE = 'Error reading from datastore:\n'
LOG_ERROR_INVALID_LINK = 'Invalid link! Link: '
LOG_ERROR_INVALID_QUICK = 'Invalid quick lookup! Reference: '
LOG_TYPE_START_NEW = 'Type: Start (new user)'
LOG_TYPE_START_EXISTING = 'Type: Start (existing user)'
LOG_TYPE_NON_TEXT = 'Type: Non-text'
LOG_UNRECOGNISED = 'Type: Unrecognised'
LOG_USER_MIGRATED = 'User {} migrated to uid {} ({})'
LOG_USER_DELETED = 'Deleted uid {} ({})'

RECOGNISED_ERROR_PARSE = 'Bad Request: Can\'t parse message text'
RECOGNISED_ERROR_MIGRATE = 'Bad Request: group chat is migrated to a supergroup chat'
RECOGNISED_ERRORS = ('PEER_ID_INVALID',
                     'Bot was blocked by the user',
                     'Forbidden: user is deleted',
                     'Forbidden: bot was kicked from the supergroup chat',
                     'Bad Request: chat not found',
                     'Bad Request: group chat is deactivated',
                     RECOGNISED_ERROR_MIGRATE)

def telegram_post(data, deadline=3):
    return urlfetch.fetch(url=TELEGRAM_URL_SEND, payload=data, method=urlfetch.POST,
                          headers=JSON_HEADER, deadline=deadline)

class User(db.Model):
    username = db.StringProperty(indexed=False)
    first_name = db.StringProperty(multiline=True, indexed=False)
    last_name = db.StringProperty(multiline=True, indexed=False)
    created = db.DateTimeProperty(auto_now_add=True)
    last_received = db.DateTimeProperty(auto_now_add=True, indexed=False)
    last_sent = db.DateTimeProperty(indexed=False)
    version = db.StringProperty(indexed=False, default='NIV')
    reply_to = db.StringProperty(multiline=True, indexed=False)
    promo = db.BooleanProperty(default=False)

    def get_uid(self):
        return self.key().name()

    def get_name_string(self):
        def prep(string):
            return string.encode('utf-8', 'ignore').strip()

        name = prep(self.first_name)
        if self.last_name:
            name += ' ' + prep(self.last_name)
        if self.username:
            name += ' @' + prep(self.username)

        return name

    def get_description(self):
        user_type = 'group' if self.is_group() else 'user'
        return user_type + ' ' + self.get_name_string()

    def is_group(self):
        return int(self.get_uid()) < 0

    def set_promo(self, promo):
        self.promo = promo
        self.put()

    def update_last_received(self):
        self.last_received = datetime.now()
        self.put()

    def update_last_sent(self):
        self.last_sent = datetime.now()
        self.put()

    def update_version(self, version):
        self.version = version
        self.put()

    def await_reply(self, command):
        self.reply_to = command
        self.put()

    def migrate_to(self, uid):
        props = dict((prop, getattr(self, prop)) for prop in self.properties().keys())
        props.update(key_name=str(uid))
        new_user = User(**props)
        new_user.put()
        self.delete()
        return new_user

def get_user(uid):
    key = db.Key.from_path('User', str(uid))
    user = db.get(key)
    if user == None:
        user = User(key_name=str(uid), first_name='-')
        user.put()
    return user

def update_profile(uid, uname, fname, lname):
    existing_user = get_user(uid)
    if existing_user:
        existing_user.username = uname
        existing_user.first_name = fname
        existing_user.last_name = lname
        existing_user.update_last_received()
        #existing_user.put()
        return existing_user
    else:
        user = User(key_name=str(uid), username=uname, first_name=fname, last_name=lname)
        user.put()
        return user

def build_buttons(menu):
    buttons = []
    for item in menu:
        buttons.append([item])
    return buttons

def build_keyboard(buttons):
    return {'keyboard': buttons, 'one_time_keyboard': True}

def send_message(user_or_uid, text, msg_type='message', force_reply=False, markdown=False,
                 disable_web_page_preview=True, custom_keyboard=None, hide_keyboard=False):
    try:
        uid = str(user_or_uid.get_uid())
        user = user_or_uid
    except AttributeError:
        uid = str(user_or_uid)
        user = get_user(user_or_uid)

    def send_short_message(text, countdown=0):
        build = {
            'chat_id': uid,
            'text': text.replace('\a', ' ')
        }

        if force_reply:
            build['reply_markup'] = {'force_reply': True}
        elif custom_keyboard:
            build['reply_markup'] = custom_keyboard
        elif hide_keyboard:
            build['reply_markup'] = {'hide_keyboard': True}
        if markdown or msg_type in ('passage', 'result'):
            build['parse_mode'] = 'Markdown'
        if disable_web_page_preview:
            build['disable_web_page_preview'] = True

        data = json.dumps(build)

        def queue_message():
            payload = json.dumps({
                'msg_type': msg_type,
                'data': data
            })
            taskqueue.add(url='/message', payload=payload, countdown=countdown)
            logging.info(LOG_ENQUEUED.format(msg_type, uid, user.get_description()))

        if msg_type in ('promo', 'mass'):
            if msg_type == 'promo':
                user.set_promo(True)

            queue_message()
            return

        try:
            result = telegram_post(data)
        except urlfetch_errors.Error as e:
            logging.warning(LOG_ERROR_SENDING.format(msg_type, uid, user.get_description(), str(e)))
            queue_message()
            return

        response = json.loads(result.content)
        error_description = str(response.get('description'))

        if error_description.startswith(RECOGNISED_ERROR_PARSE):
            if build.get('parse_mode'):
                del build['parse_mode']
            data = json.dumps(build)
            queue_message()

        elif handle_response(response, user, uid, msg_type) == False:
            queue_message()

    if text.strip() == '':
        return

    if len(text) > 4096:
        chunks = textwrap.wrap(text, width=4096, replace_whitespace=False, drop_whitespace=False)
        i = 0
        for chunk in chunks:
            send_short_message(chunk, i)
            i += 1
    else:
        send_short_message(text)

def handle_response(response, user, uid, msg_type):
    if response.get('ok') == True:
        msg_id = str(response.get('result').get('message_id'))
        logging.info(LOG_SENT.format(msg_type.capitalize(), msg_id, uid, user.get_description()))
        user.update_last_sent()

    else:
        error_description = str(response.get('description'))
        if error_description not in RECOGNISED_ERRORS:
            logging.warning(LOG_ERROR_SENDING.format(msg_type, uid, user.get_description(),
                                                     error_description))
            return False

        logging.info(LOG_DID_NOT_SEND.format(msg_type, uid, user.get_description(),
                                             error_description))
        if error_description == RECOGNISED_ERROR_MIGRATE:
            new_uid = response.get('parameters', {}).get('migrate_to_chat_id')
            if new_uid:
                user = user.migrate_to(new_uid)
                logging.info(LOG_USER_MIGRATED.format(uid, new_uid, user.get_description()))
        else:
            user_description = user.get_description()
            user.delete()
            logging.info(LOG_USER_DELETED.format(uid, user_description))
            return True

        if msg_type == 'promo':
            user.set_promo(False)

    return True

def send_typing(uid):
    data = json.dumps({'chat_id': uid, 'action': 'typing'})
    try:
        rpc = urlfetch.create_rpc()
        urlfetch.make_fetch_call(rpc, url=TELEGRAM_URL_CHAT_ACTION, payload=data,
                                 method=urlfetch.POST, headers=JSON_HEADER)
    except urlfetch_errors.Error:
        return

class MainPage(webapp2.RequestHandler):
    BOT_USERNAME = 'biblegatewaybot'
    BOT_HANDLE = '@' + BOT_USERNAME
    BOT_DESCRIPTION = 'This bot can fetch bible passages from biblegateway.com.'

    CMD_LIST = '/get <reference>\n/get<version> <reference>\n' + \
               '/search <keyword>\n/setdefault <version>\n\n' + \
               'Examples:\n/get John 3:16\n/getNLT 1 cor 13:4-7\n' + \
               '/search the greatest commandment\n/setdefault NASB'

    WELCOME_GROUP = 'Hello, friends in {}! Thanks for adding me in!'
    WELCOME_USER = 'Hello, {}! Welcome!'
    WELCOME_GET_STARTED = ' ' + BOT_DESCRIPTION + \
                          '\n\nTo get started, enter one of the following commands:\n' + CMD_LIST

    HELP = 'Hi {}! Please enter one of the following commands:\n' + CMD_LIST + '\n\n' + \
           'Enjoy using BibleGateway Bot? Click the link below to rate it!\n' + \
           'https://telegram.me/storebot?start=' + BOT_USERNAME

    UNRECOGNISED = 'Sorry {}, I couldn\'t understand that. ' + \
                   'Please enter one of the following commands:\n' + CMD_LIST

    REMOTE_ERROR = 'Sorry {}, I\'m having some difficulty accessing the site. ' + \
                   'Please try again later.'

    GET_PASSAGE = 'Which bible passage do you want to lookup? Version: {}\n\n' + \
                  'Tip: For faster results, use:\n/get John 3:16\n/get{} John 3:16'

    GET_SEARCH_TERM = 'Please enter what you wish to search for.\n\n' + \
                      'Tip: For faster results, use:\n/search make disciples\n' + \
                      '/search "love is patient" _(quotes to match exact phrase)_'

    NO_RESULTS_FOUND = 'Sorry {}, no results were found. Please try again.'
    VERSION_NOT_FOUND = 'Sorry {}, I couldn\'t find that version. ' + \
                        'Use /setdefault to view all available versions.'

    SET_DEFAULT_CHOOSE_LANGUAGE = 'Choose a language:'
    SET_DEFAULT_CHOOSE_VERSION = 'Select a version:'
    SET_DEFAULT_SUCCESS = 'Success! Default version is now *{}*.'
    SET_DEFAULT_FAILURE = VERSION_NOT_FOUND + '\n\nCurrent default is *{}*.'

    SETTINGS = 'Current default version is *{}*. Use /setdefault to change it.'

    BACK_TO_LANGUAGES = u'\U0001F519' + ' to language list'

    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.write(self.BOT_USERNAME + ' backend running...\n')

    def post(self):
        data = json.loads(self.request.body)
        logging.debug(self.request.body)

        msg = data.get('message')
        msg_chat = msg.get('chat')
        msg_from = msg.get('from')

        uid = str(msg_chat.get('id'))
        first_name = msg_from.get('first_name')
        last_name = msg_from.get('last_name')
        username = msg_from.get('username')

        name = first_name.encode('utf-8', 'ignore').strip()
        text = msg.get('text')
        raw_text = text
        if text:
            text = text.encode('utf-8', 'ignore')
            logging.info(text)

        if msg_chat.get('type') == 'private':
            group_name = name
            user = update_profile(uid, username, first_name, last_name)
        else:
            group_name = msg_chat.get('title')
            user = update_profile(uid, None, group_name, None)
            group_name = group_name.encode('utf-8', 'ignore')

        if text == '/botfamily_verification_code':
            send_message(user, BOTFAMILY_HASH)
            send_message(ADMIN_ID, 'Botfamily verified! :D')
            return

        def get_from_string():
            name_string = name
            if last_name:
                name_string += ' ' + last_name.encode('utf-8', 'ignore').strip()
            if username:
                name_string += ' @' + username.encode('utf-8', 'ignore').strip()
            return name_string

        if user.last_sent == None or text == '/start':
            if user.last_sent == None:
                logging.info(LOG_TYPE_START_NEW)
                new_user = True
            else:
                logging.info(LOG_TYPE_START_EXISTING)
                new_user = False

            if user.is_group():
                response = self.WELCOME_GROUP.format(group_name)
            else:
                response = self.WELCOME_USER.format(name)
            response += self.WELCOME_GET_STARTED
            send_message(user, response, msg_type='welcome')
            user.await_reply(None)

            if new_user:
                if user.is_group():
                    new_alert = 'New group: "{}" via user: {}'.format(group_name, get_from_string())
                else:
                    new_alert = 'New user: ' + get_from_string()
                send_message(ADMIN_ID, new_alert)

            return

        if text == None:
            logging.info(LOG_TYPE_NON_TEXT)
            migrate_to_chat_id = msg.get('migrate_to_chat_id')
            if migrate_to_chat_id:
                new_uid = migrate_to_chat_id
                user = user.migrate_to(new_uid)
                logging.info(LOG_USER_MIGRATED.format(uid, new_uid, user.get_description()))
            return
        text = text.strip()

        def is_get_command():
            return text.lower().startswith('/get')

        def is_full_set_default_command():
            return text.lower().startswith('/setdefault ')

        def is_full_search_command():
            return text.lower().startswith('/search ')

        def is_link_command():
            return text[1:].startswith(BOOKS)

        def is_command(word):
            cmd = text.lower().strip()
            short_cmd = ''.join(cmd.split())
            slash_word = '/' + word
            left_pattern = slash_word + self.BOT_HANDLE
            right_pattern = self.BOT_HANDLE + slash_word
            return cmd == slash_word or short_cmd.startswith((left_pattern, right_pattern))

        if is_command('get'):
            user.await_reply('get')
            version = user.version
            send_message(user, self.GET_PASSAGE.format(version, other_version(version)),
                         force_reply=True)

        elif is_command('search'):
            user.await_reply('search')
            send_message(user, self.GET_SEARCH_TERM, force_reply=True, markdown=True)

        elif is_get_command():
            user.await_reply(None)
            words = text.split()
            first_word = words[0]

            version = first_word[4:].upper()
            if not version:
                version = user.version
            if version not in VERSIONS:
                send_message(user, self.VERSION_NOT_FOUND.format(name))
                return

            passage = text[len(first_word) + 1:].strip()
            if not passage:
                user.await_reply(first_word[1:])
                send_message(user, self.GET_PASSAGE.format(version, other_version(version)),
                             force_reply=True)
                return
            first_passage_word = passage.split()[0].upper()
            if len(first_word) == 4 and first_passage_word in VERSIONS and \
               passage[len(first_passage_word) + 1:].strip():
                version = first_passage_word
                passage = passage[len(first_passage_word) + 1:]

            send_typing(uid)
            response = get_passage(passage, version)

            if response == EMPTY:
                send_message(user, self.NO_RESULTS_FOUND.format(name))
                return
            elif response == None:
                send_message(user, self.REMOTE_ERROR.format(name))
                return

            send_message(user, response, msg_type='passage')

        elif is_full_set_default_command():
            user.await_reply(None)
            version = text[12:].strip().upper()

            if version not in VERSIONS:
                send_message(user, self.SET_DEFAULT_FAILURE.format(name, user.version),
                             markdown=True)
                return

            user.update_version(version)
            send_message(user, self.SET_DEFAULT_SUCCESS.format(version), markdown=True)

        elif is_full_search_command():
            search_term = text[8:].strip().lower()
            user.await_reply('search0 ' + raw_text[8:].strip().lower())

            send_typing(uid)
            response = get_search_results(search_term)

            if response == EMPTY:
                user.await_reply(None)
                send_message(user, self.NO_RESULTS_FOUND.format(name))
                return
            elif response == None:
                send_message(user, self.REMOTE_ERROR.format(name))
                return

            send_message(user, response, msg_type='result')

        elif is_command('setdefault') or raw_text == self.BACK_TO_LANGUAGES:
            user.await_reply(None)
            buttons = build_buttons(VERSION_DATA.keys())
            keyboard = build_keyboard(buttons)
            send_message(user, self.SET_DEFAULT_CHOOSE_LANGUAGE, custom_keyboard=keyboard)

        elif raw_text in VERSION_DATA:
            user.await_reply(None)
            buttons = build_buttons(VERSION_DATA[raw_text] + [self.BACK_TO_LANGUAGES])
            keyboard = build_keyboard(buttons)
            send_message(user, self.SET_DEFAULT_CHOOSE_VERSION, custom_keyboard=keyboard)

        elif raw_text in VERSION_LOOKUP:
            user.await_reply(None)
            version = VERSION_LOOKUP[raw_text]
            user.update_version(version)
            send_message(user, self.SET_DEFAULT_SUCCESS.format(version), markdown=True,
                         hide_keyboard=True)

        elif is_command('help'):
            user.await_reply(None)
            send_message(user, self.HELP.format(name))

        elif is_command('settings'):
            user.await_reply(None)
            send_message(user, self.SETTINGS.format(user.version), markdown=True)

        elif is_link_command():
            user.await_reply(None)
            passage = text[1:].replace('V', ':')
            if passage.endswith(self.BOT_HANDLE):
                passage = passage[:-len(self.BOT_HANDLE)]

            send_typing(uid)
            response = get_passage(passage, user.version)

            if response == EMPTY:
                send_message(user, self.NO_RESULTS_FOUND.format(name))
                logging.info(LOG_ERROR_INVALID_LINK + text)
                return
            elif response == None:
                send_message(user, self.REMOTE_ERROR.format(name))
                return

            send_message(user, response, msg_type='passage')

        elif text in ('/more', '/more' + self.BOT_HANDLE) and user.reply_to != None and \
             user.reply_to.startswith('search'):
            idx = user.reply_to.find(' ')
            old_start = int(user.reply_to[6:idx])
            search_term = user.reply_to[idx + 1:]

            new_start = old_start + MAX_SEARCH_RESULTS

            user.await_reply('search{} '.format(new_start) + search_term)

            send_typing(uid)
            response = get_search_results(search_term, new_start)

            if response == EMPTY:
                user.await_reply(None)
                send_message(user, self.NO_RESULTS_FOUND.format(name))
                return
            elif response == None:
                send_message(user, self.REMOTE_ERROR.format(name))
                return

            send_message(user, response, msg_type='result')

        elif user.reply_to != None and user.reply_to == 'search':
            search_term = text
            user.await_reply('search0 ' + raw_text)

            send_typing(uid)
            response = get_search_results(search_term)

            if response == EMPTY:
                user.await_reply(None)
                send_message(user, self.NO_RESULTS_FOUND.format(name))
                return
            elif response == None:
                send_message(user, self.REMOTE_ERROR.format(name))
                return

            send_message(user, response, msg_type='result')

        elif user.reply_to != None and user.reply_to.startswith('get'):
            version = user.reply_to[3:].upper()
            user.await_reply(None)
            if not version:
                version = user.version

            send_typing(uid)
            response = get_passage(text, version)

            if response == EMPTY:
                send_message(user, self.NO_RESULTS_FOUND.format(name))
                return
            elif response == None:
                send_message(user, self.REMOTE_ERROR.format(name))
                return

            send_message(user, response, msg_type='passage')

        else:
            user.await_reply(None)
            msg_reply = msg.get('reply_to_message')
            if user.is_group() and self.BOT_HANDLE not in text and \
               not (msg_reply and str(msg_reply.get('from').get('id')) == BOT_ID):
                logging.info(LOG_UNRECOGNISED)
                return

            to_lookup = text.lower().replace(self.BOT_HANDLE, '')
            to_lookup = to_lookup.replace('revelations', 'revelation')
            refs = extract_refs(to_lookup)
            if refs:
                ref = refs[0]
                book = ref[0]
                if book == 'Revelation of Jesus Christ':
                    book = 'Revelation'
                passage = '{} {}:{}-{}:{}'.format(book, ref[1], ref[2], ref[3], ref[4])

                send_typing(uid)
                response = get_passage(passage, user.version)

                if response == EMPTY:
                    logging.error(LOG_ERROR_INVALID_QUICK + text)

                if response and response != EMPTY:
                    send_message(user, response, msg_type='passage')
                    return

            logging.info(LOG_UNRECOGNISED)
            send_message(user, self.UNRECOGNISED.format(name))

class MessagePage(webapp2.RequestHandler):
    def post(self):
        params = json.loads(self.request.body)
        msg_type = params.get('msg_type')
        data = params.get('data')
        uid = str(json.loads(data).get('chat_id'))
        user = get_user(uid)

        try:
            result = telegram_post(data, 4)
        except urlfetch_errors.Error as e:
            logging.warning(LOG_ERROR_SENDING.format(msg_type, uid, user.get_description(), str(e)))
            logging.debug(data)
            self.abort(502)

        response = json.loads(result.content)

        if handle_response(response, user, uid, msg_type) == False:
            logging.debug(data)
            self.abort(502)

class MigratePage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.write('Migrate page\n')

class PromoPage(webapp2.RequestHandler):
    def get(self):
        taskqueue.add(url='/promo')

    def post(self):
        three_days_ago = datetime.now() - timedelta(days=3)
        query = User.all()
        query.filter('promo =', False)
        query.filter('created <', three_days_ago)
        for user in query.run(batch_size=500):
            name = user.first_name.encode('utf-8', 'ignore').strip()
            if user.is_group():
                promo_msg = 'Hello, friends in {}! '.format(name) + \
                'Do you find BibleGateway Bot useful?'
            else:
                promo_msg = 'Hi {}, do you find BibleGateway Bot useful?'.format(name)
            promo_msg += ' Why not rate it on the bot store (you don\'t have to exit' + \
                         ' Telegram)!\nhttps://telegram.me/storebot?start=biblegatewaybot'
            send_message(user, promo_msg, msg_type='promo')

app = webapp2.WSGIApplication([
    ('/', MainPage),
    ('/' + TOKEN, MainPage),
    ('/message', MessagePage),
    ('/promo', PromoPage),
    ('/migrate', MigratePage),
], debug=True)
