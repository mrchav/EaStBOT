from datetime import datetime
import pymysql, time
import aiogram
import asyncio
from telebot import types
import aioschedule
import yfinance as yf
import config as cf
from aiogram.types import ReplyKeyboardRemove, \
    ReplyKeyboardMarkup, KeyboardButton, \
    InlineKeyboardMarkup, InlineKeyboardButton

MSG_LIST = {
    'start_msg': 'Hi, this bot will help you track stock prices. Just add a task and when the share price reaches the target value, the bot will tell you. Example: \nformat for add task: \n/add tiker targetprice days comment \n/add APPL 110 3 comment1\nto check the active tasks, type',
    'no_tasks' : 'you have no active tasks',
    'add_task': 'new task add',
    'no_tiker': 'tiker not found',
    'wrong_format': 'message have wrong format\n',
    'wrong_add' : '\nformat for add task: \n/add tiker targetprice days comment \nнапример(сигнал для акций APPL при цене 10уе актуальность таска 3 дня)\nкоманда должна выглядеть следующим образом\n/add APPL 10 3 comment1',
    'help_msg': 'Hi, this bot will help you track stock prices. Just add a task and when the share price reaches the target value, the bot will tell you. Example: \nformat for add task: \n/add tiker targetprice days comment \n/add APPL 110 3 comment1\nto check the active tasks, type',
    'del_no_task_id': 'true format for del task /del_taskid \n example:\n/del_63232',
    'del_task_id': ' task delete',
    'wrong_id_format': 'wrong id format'
}

BOT_API = cf.BOT_API

str_true_mes_format = '\nformat for add task: \n/add tiker targetprice days comment \nнапример(сигнал для акций APPL при цене 10уе актуальность таска 3 дня)\nкоманда должна выглядеть следующим образом\n/add APPL 10 3 comment1'

bot = aiogram.Bot(token=BOT_API)
dp = aiogram.Dispatcher(bot)

con = pymysql.connect(db='easy_stocks', user='root', passwd='112233', host='localhost', port=3306)

def print_log(message, command):
    print(f'{datetime.now().strftime("%d.%m.%Y %H:%M:%S")} '
          f'command:{command} user:{message.from_user.id} message:{message.text} ')


def load_tiker(message_data, con, new_tiker = ''):
    if new_tiker == '':
        ttiker = message_data["tiker"]
    else:
        ttiker = new_tiker
    cur = con.cursor()
    cur.execute("SELECT id FROM all_stock_tikers WHERE ticker_name = %s", (ttiker,))
    count = cur.fetchone()

    if count is not None:
        message_data['tiker_id'] = count[0]
        return message_data
    else:
        return False

def add_new_user(data,con):
    #проверяем, если ли пользователь в базе
    cur = con.cursor()
    cur.execute("SELECT * FROM easy_stocks.users WHERE tm_user_id = %s", (data.from_user.id,))
    count = cur.fetchone()
    if count is None:
        cur = con.cursor()
        cur.execute(
            "INSERT INTO easy_stocks.users (tm_user_id, is_bot, tm_first_name, tm_last_name, tm_username, tm_language_code, start_date  ) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (data.from_user.id, data.from_user.is_bot, data.from_user.first_name, data.from_user.last_name,
             data.from_user.username, data.from_user.language_code, datetime.fromtimestamp(time.time())))
        con.commit()
    else:
        print('пользователь уже есть в базе, добавлять не будем')


def prepare_message_add(message):
    data = message.split(" ")
    res_data = {}
    res_data['comm'] = ''
    if (len(data) >= 4):
        try:
            target_price = float(data[2])
        except:
            target_price = False
        if ((data[2].isdigit()) or (type(target_price) == float)) & (data[3].isdigit()):
            res_data['tiker'] = data[1]
            res_data['target_price'] = target_price
            res_data['stop_alert'] = data[3]
            res_data['chek'] = 'True'
            if len(data) > 4:
                print(len(data))
                for i in range(4, len(data)):
                    res_data['comm'] = res_data['comm'] + ' ' + data[i]
        else:
            res_data['chek'] = 'False'
        if not chek_tiker_in_bd(res_data['tiker']):
            new_tiker = add_new_tiker_to_bd(res_data['tiker'])
            if new_tiker:
                res_data['tiker'] = new_tiker
    else:
        res_data['chek'] = 'False'
    return res_data

def add_new_tiker_to_bd(tiker):
    data = check_tiker_in_yadata(tiker)
    print(f'вот что достали с яху{data}')
    if data:
        print(f'добавляем новый тикер в базу {tiker}')
        cur = con.cursor()
        cur.execute(
            "INSERT INTO easy_stocks.all_stock_tikers (ticker_name,tiker_full_name,has_yahoo_data) VALUES (%s,%s,%s)",
            (data['tiker'], data['tiker_full_name'],1))
        con.commit()
        return data['tiker']
    else:
        ru_tiker = tiker+'.me'
        data = check_tiker_in_yadata(ru_tiker)
        print(f'вторая попытка достать с яху{data}')
        if data:
            print(f'добавляем новый тикер в базу {ru_tiker}')
            cur = con.cursor()
            cur.execute(
                "INSERT INTO easy_stocks.all_stock_tikers (ticker_name,tiker_full_name,has_yahoo_data) VALUES (%s,%s,%s)",
                (ru_tiker, data['tiker_full_name'], 1))
            con.commit()
        return ru_tiker




def chek_tiker_in_bd(tiker):
    cur = con.cursor()
    cur.execute("SELECT id FROM easy_stocks.all_stock_tikers WHERE ticker_name = %s", (tiker,))
    row = cur.fetchone()
    # print()
    if row is None:
        return False
    else:
        return True



def add_new_task(data, message_data, con):
    '''
     проверяем, есть ли пользователь, который добаляет таск
     '''
    cur = con.cursor()
    cur.execute("SELECT id FROM easy_stocks.users WHERE tm_user_id = %s", (data.from_user.id,))
    row = cur.fetchone()
    #print()
    if row is None:
        print('пользователь не найден')
    else:
        max_period = datetime.fromtimestamp(time.time() + 86400*int(message_data['stop_alert']))
        cur1 = con.cursor()
        cur1.execute(
            "INSERT INTO easy_stocks.stoks_tasks "
            "(users_id, price_target, max_period, all_stock_tikers_id, comment, task_active) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (row[0], message_data['target_price'], max_period, message_data['tiker_id'], message_data['comm'], 1))
        cur = con.cursor()
        cur.execute(
            "INSERT INTO easy_stocks.tasks_log (add_time, tiker_id, users_id, task_id) VALUES (%s,%s,%s,%s)",
            (datetime.fromtimestamp(time.time()), message_data['tiker_id'], row[0], cur1.lastrowid))
        con.commit()

async def chek_expired_task():
    start_time = datetime.fromtimestamp(time.time())
    cur = con.cursor()
    cur.execute("SELECT id, max_period FROM stoks_tasks as st WHERE st.task_active = 1", ())
    for row in cur.fetchall():
        if row[1] < datetime.fromtimestamp(time.time()):
            cur.execute("UPDATE stoks_tasks SET task_active = 0, expired = 1 WHERE id = %s ",
                        (row[0],))
    con.commit()
    #print(f'проверяем истекшие таски в базе  |  chek_expired_task| всего проверяли сек:{datetime.fromtimestamp(time.time())-start_time} ')


async def show_active_alerts(user_id):
    cur = con.cursor()
    cur.execute("SELECT all_st.ticker_name, st.price_target, st.comment, st.max_period, st.id FROM stoks_tasks as st "
                "LEFT JOIN users as u on st.users_id = u.id "
                "LEFT JOIN all_stock_tikers as all_st on all_st.id = st.all_stock_tikers_id "
                "WHERE u.tm_user_id = %s and st.task_active = %s", (user_id, 1))
    bd_data = cur.fetchall()
    if len(bd_data) > 0:
        msg = 'active tasks:'
    else:
        msg = MSG_LIST['no_tasks']
    for row in bd_data:
        msg = f'{msg} \n tiker:{row[0]} target price:{row[1]}  task expires:{row[3].strftime("%d.%m %H:%M")} You comment:{row[2]} /DEL_{row[4]}'

    await bot.send_message(user_id, msg)



async def load_active_alerts():
    cur = con.cursor()
    cur.execute("SELECT st.id, st.price_target, u.tm_user_id,st.current_price, st.m_l, all_st.ticker_name "
                "FROM stoks_tasks as st "
                "LEFT JOIN users as u on st.users_id = u.id "
                "LEFT JOIN all_stock_tikers as all_st on all_st.id = st.all_stock_tikers_id "
                "WHERE  st.task_active = %s and st.max_period > %s", (1, datetime.fromtimestamp(time.time())))
    return cur.fetchall()

def check_tiker_in_yadata(tiker):
    data = {}
    try:
        t_data = yf.Ticker(tiker)
        data['tiker'] = tiker
        data['tiker_full_name'] = t_data.info['shortName']
        data['stock_exchenge_name'] = t_data.info['exchange']
        return data
    except:
        return False



@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    print_log(message, 'start')
    add_new_user(message, con)
    await message.answer(MSG_LIST['start_msg'])


@dp.message_handler(commands=['help'])
async def send_help(message: types.Message):
    print_log(message, 'help')
    await message.answer(MSG_LIST['help_msg'])


@dp.message_handler(commands=['add'])
async def add_message(message: types.Message):
    print_log(message, 'add')
    message_data = prepare_message_add(message.text)
    if message_data['chek'] == 'True':
        if load_tiker(message_data, con):
            add_new_task(message, message_data, con)
            bot_response = MSG_LIST['add_task']
        else:
            bot_response = MSG_LIST['no_tiker']
    else:
        bot_response = MSG_LIST['wrong_format'] + MSG_LIST['wrong_add']
    await message.answer(bot_response)


@dp.message_handler(commands=['activealerts'])
async def active_alerts(message: types.Message):
    print_log(message, 'activealerts')
    await show_active_alerts(message.from_user.id)


@dp.message_handler(regexp="/DEL")
async def del_alerts(message: types.Message):
    print_log(message, 'del')
    task_id = message.text.replace('/DEL_', '').replace('/del_', '')
    if len(task_id) > 0:
        if (task_id.isalnum()):
            await del_alet_in_base(message.from_user.id, task_id)
            await bot.send_message(message.from_user.id, MSG_LIST['del_task_id'])
        else:
            message.answer(MSG_LIST['wrong_id_format'])
    else:
        await message.answer(MSG_LIST['del_no_task_id'])


async def del_alet_in_base(user,task):
    cur = con.cursor()
    cur.execute("UPDATE stoks_tasks as st LEFT JOIN users as u on st.users_id = u.id "
                "SET st.task_active = 0 WHERE u.tm_user_id = %s and st.id = %s",
                (user, task))
    con.commit()

    await show_active_alerts(user)
    #await show_active_alerts(message)



async def chek_null_current_price():
    cur = con.cursor()
    cur1 = con.cursor()
    cur.execute("SELECT st.id, ast.ticker_name, st.price_target FROM stoks_tasks as st "
                "LEFT JOIN all_stock_tikers as ast ON ast.id = st.all_stock_tikers_id "
                "WHERE st.current_price is NULL or st.current_price = %s;",
                (0,))
    bd_data = cur.fetchall()
    if len(bd_data) > 0:
        for row in bd_data:
            target_price = row[2]
            tiker_price = download_current_price(row[1])
            if target_price > tiker_price:
                ml = -1
            else:
                ml = 1
            cur1.execute("UPDATE stoks_tasks SET current_price = %s, m_l = %s, goal = 1 WHERE id = %s",
                        (tiker_price, ml, row[0]))
        con.commit()

async def alert_done(data):
    await del_alet_in_base(data[2], data[0])
    await bot.send_message(data[2], f' цена достигла цели alert_id:{data[0]} tiker:{data[5]} target price:{data[1]}')


def download_current_price(tiker):
    t_data = yf.Ticker(tiker)
    data = t_data.history(period="1d", interval="1m")
    last_quote = (data.tail(1)['Close'].iloc[0])
    return last_quote


async def check_tasks_prices(n):
    act_alerts = await load_active_alerts()
    #print(act_alerts)
    #print(f'прошло еще  сек. Время: {datetime.now().strftime("%d.%m.%Y %H:%M:%S")}')
    for row in act_alerts:
        '''
        [0] id алерта
        [1] target price
        [2] user id
        [3] start price
        [4] m_l если -1 то цена должна подняться до таргет цены . если 1, то цена должна опуститься 
        [5] tiker name 
        '''
        current_price = download_current_price(row[5])

        if row[4] > 0: #если m_l > 0, то триггер сработает когда цена опустится ниже таргета
            if row[1] < current_price:  # row[1] цена которую поставил пользователь
                await alert_done(row)

        if row[4] < 0:
            if row[1] > current_price:
                await alert_done(row)



async def scheduler():
    x = 60
    aioschedule.every(x).seconds.do(check_tasks_prices, n=x)
    aioschedule.every(x).seconds.do(chek_null_current_price)
    aioschedule.every(x).seconds.do(chek_expired_task)

    while True:
            await aioschedule.run_pending()
            await asyncio.sleep(1)


async def on_startup(x):
    print('on start up')
    asyncio.create_task(scheduler())


if __name__ == '__main__':
    print('start')
    #dp.loop.create_task(check_tasks_prices())
    aiogram.executor.start_polling(dp, skip_updates=True, on_startup=on_startup)



