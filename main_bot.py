import time
import aiogram.utils.markdown as md
from aiogram import Bot, Dispatcher
from aiogram.types import CallbackQuery, Message,\
    InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Text
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import ParseMode
from aiogram.utils import executor
from aiogram import Bot
import os
import vk
from sqlalchemy import create_engine
import pandas as pd
from vk_api import exceptions as exp
import asyncio

print(1)
TOKEN = "xxx"
CHAT_ID = 586779742
bot = Bot(token=TOKEN)
print(2)
session = vk.Session(access_token="xxxx")
vk_api = vk.API(session, v='5.131')  # подключаем вк
storage = MemoryStorage()

dp = Dispatcher(bot, storage=storage)
conn_pg = create_engine('postgresql+psycopg2://xxxx')
base_df1 = pd.read_sql("""select owner_id, albums from described where id in (1,2,3)""", con=conn_pg)
# print(base_df1)
owner_id = str(base_df1.iloc[0][0])
album_id = base_df1.iloc[1][1]
end_album_id = base_df1.iloc[2][1]
print(3)
# base_df1 = pd.read_sql("""select albums from described where id=2""", con=conn_pg)
# album_id = base_df1.iloc[0][0]

# создаём форму и указываем поля
class Form(StatesGroup):
    # date = State()
    job = State()
    price = State()
    selfprice = State()
    owner = State()

async def scheduled_backup_file(wait_for):
    while True:
        df = pd.read_sql("""select bbc.id, name, price, selfprice, dd.status, amount, kind as seria, profit, client, data_contact, ddd.method_sale, date_sale,
                               photo_id1, photo_id2, comment_id1, comment_id2, photo_url1, photo_url2
                            from main_base_coins bbc
                            left join 
                                (select id, kind from described) d
                                on d.id = bbc.seria
                            left join 
                                (select id, status from described) dd
                                on dd.id = bbc.status
                            left join 
                                (select id, method_sale from described) ddd
                                on ddd.id = bbc.method_sale
                            order by bbc.id
                        """, con=conn_pg)
        xl = pd.ExcelWriter(os.path.join(os.getcwd(),"backup_coins_base.xlsx"), engine='xlsxwriter')
        # Load a sheet into a DataFrame by name: df1
        for i, defi in enumerate([df]):#прогоняем список чтобы загнать каждый датафрейм в отдельную страницу экселя
            defi.to_excel(
                xl,
                sheet_name='Sheet1',
                index=False) #убираем первый столбик индексов
        xl.save()#сохранить в файл
        await asyncio.sleep(wait_for)
        print('back_up')

async def scheduled_replace_photo(wait_for):
    'Перенос фоток в альбом продаж - окончательно проданная монета'
    while True:
        df = pd.read_sql("""select bbc.id, photo_id1, photo_id2, bbc.status, ddd.method_sale
                            from main_base_coins bbc
                            left join 
                                (select id, method_sale from described) ddd
                                on ddd.id = bbc.method_sale
                            where bbc.status = 4
                            order by bbc.id
                        """, con=conn_pg)
        if len(df) >= 1:
            for index, row in df.iterrows():
                time.sleep(12)
                place = row['method_sale']
                while True:
                    try:
                        vk_api.photos.createComment(owner_id=owner_id, photo_id=row['photo_id1'], message=f'Продано на {place}')
                        break
                    except:
                        time.sleep(3)
                        continue
                time.sleep(2)
                while True:
                    try:
                        vk_api.photos.createComment(owner_id=owner_id, photo_id=row['photo_id2'], message=f'Продано на {place}')
                        break
                    except:
                        time.sleep(3)
                        continue
                while True:
                    try:
                        vk_api.photos.move(owner_id=owner_id, photo_id=row['photo_id1'], target_album_id=end_album_id)  # Перемещение в альбом
                        vk_api.photos.move(owner_id=owner_id, photo_id=row['photo_id2'], target_album_id=end_album_id)
                        break
                    except:
                        time.sleep(3)
                        continue
                ids = str(row['id'])
                query = """ update main_base_coins as f
                                set status = 5, photo_url1 = null, photo_url2 = null
                            where f.id = """+ids+"""::integer"""
                with conn_pg.begin() as conn:
                    conn.execute(query)
        await asyncio.sleep(wait_for)
        print('replace_vk')

@dp.message_handler(state='*', commands='cancel')
@dp.message_handler(Text(equals='отмена', ignore_case=True), state='*')
async def cancel_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is not None:
        await state.finish()
    await message.reply('ОК')

@dp.message_handler(content_types=['text'])
async def date_start(message: Message):
    base_df = pd.read_sql("""select id, name, price, photo_id1, photo_id2, photo_url1, photo_url2, selfprice from main_base_coins where status = 2 and name ilike %(slo)s order by dates""", con=conn_pg, params={'slo': f'%{message.text}%'})
    base_df.reset_index(inplace=True)  # make sure indexes pair with number of rows
    if len(base_df) == 0:
        await bot.send_message(message.chat.id, "Ничего не найдено")
    else:
        await bot.send_message(message.chat.id, f"Найдено {len(base_df)} штук")
    for index, row in base_df.iterrows():
        if index == 25:
            break
        b1 = InlineKeyboardButton("Забронировать",
                                            callback_data="move {} {} {}".format(row['id'], row['photo_id1'], row['photo_id2']))
        b2 = InlineKeyboardButton("Изменить цену",
                                            callback_data="price {} {} {}".format(row['id'], row['photo_id1'], row['photo_id2']))
        b3 = InlineKeyboardButton("Изменить себестоимость",
                                            callback_data="value {} {} {}".format(row['id'], row['photo_id1'], row['photo_id2']))
        b4 = InlineKeyboardButton("Изменить владельца",
                                            callback_data="owner {} {} {}".format(row['id'], row['photo_id1'], row['photo_id2']))
        b5 = InlineKeyboardButton("Удалить",
                                            callback_data="del {} {} {}".format(row['id'], row['photo_id1'], row['photo_id2']))
        kb = InlineKeyboardMarkup(row_width=2).add(b1, b4, b3, b5, b2)  # Создаем клавиатуру
        try:
            await bot.send_media_group(message.chat.id, [InputMediaPhoto(row['photo_url1']),
                                                    InputMediaPhoto(row['photo_url2'])])  # Отсылаем сразу 2 фото
        except:
            print('RError Photo')
        await bot.send_message(message.chat.id, f"{row['name']}\nЦена: {row['price']}\nСебестоимость: {row['selfprice']}",
                                   reply_markup=kb)  # Отсылаем клавиатуру
        # await Form.job.set()

# @dp.message_handler(lambda message: message.text not in ["Забронировать", "Изменить цену", "Изменить себестоимость"])
# async def comman_invalid(message: Message):
#     logger.error(f'invalid command')
#     return await message.reply("Не знаю такой команды. Укажи команду кнопкой на клавиатуре")

@dp.callback_query_handler()
async def process_stack(call: CallbackQuery, state: FSMContext):
    ff = call.data
    sp = ff.split(" ")
    action = sp[0]
    id = sp[1]
    url1 = int(sp[2])
    url2 = int(sp[3])
    # base_df = pd.read_sql("""select owner_id from described where id=1""", con=conn_pg)
    # owner_id = str(base_df.iloc[0][0])
    await Form.job.set()
    async with state.proxy() as data:
        data['job'] = [int(id), url1, url2]#формируем номер группы + номер поста
    # async with state.proxy() as data:
    #     data['job'] = url#формируем номер группы + номер поста
    # global SLIP
    try:
        await bot.edit_message_text(call.message.text, message_id=call.message.message_id,
                                chat_id=call.message.chat.id)
    except:
        x = 0
    if action == 'move':
        await bot.answer_callback_query(call.id, "Перемещаю фотографии...", show_alert=True)
        while(True):
            try:
                vk_api.photos.move(owner_id=owner_id, photo_id=url1, target_album_id=album_id)  # Перемещение в альбом
                vk_api.photos.move(owner_id=owner_id, photo_id=url2, target_album_id=album_id)
                break            
            except:
                time.sleep(2)
                continue
        #меняем статус в таблице на бронь
        query = """update main_base_coins as f
            set status = 3, comment_id1 = null, comment_id2 = null,
                date_sale = CURRENT_TIMESTAMP
            where f.id = """+id+"""::integer"""
        with conn_pg.begin() as conn:
            conn.execute(query)
        await bot.send_message(call.message.chat.id, "Фотографии успешно перенесены.")
        await state.finish()
    if action == "price":
        # Ожидание цены
        # await bot.answer_callback_query(call.id, "Ожидайте...", show_alert=False)
        await bot.send_message(call.message.chat.id, "Пожалуйста, укажите новую цену.")
        await Form.next()
    if action == "value":
        # Ожидание себестоимости
        await bot.send_message(call.message.chat.id, "Пожалуйста, укажите новую себестоимость.")
        await Form.next()
        await Form.next()
    if action == "owner":
        # Ожидание владелец
        await bot.send_message(call.message.chat.id, "Пожалуйста, введите нового владельца.")
        await Form.next()
        await Form.next()
        await Form.next()
    if action == "del":
        # Полное удаление
        await bot.answer_callback_query(call.id, "Удаляю фотографии...", show_alert=True)
        while(True):
            try:
                vk_api.photos.delete(owner_id=owner_id, photo_id=url1)  # удаляем первую фотку
                break            
            except:
                time.sleep(2)
                continue
        while(True):
            try:
                vk_api.photos.delete(owner_id=owner_id, photo_id=url2) # удаляем вторую фотку
                break            
            except:
                time.sleep(2)
                continue
        #меняем статус в таблице на бронь
        query = """DELETE FROM main_base_coins  f
                    where f.id = """+id+"""::integer"""
        with conn_pg.begin() as conn:
            conn.execute(query)
        await bot.send_message(call.message.chat.id, "Фотографии успешно удалены.")
        await state.finish()

@dp.message_handler(lambda message: not message.text.isdigit(), state=Form.price)
async def stack_invalid(message: Message):
    return await message.reply("Напиши число или напиши /cancel")

@dp.message_handler(lambda message: not message.text.isdigit(), state=Form.selfprice)
async def stack_invalid(message: Message):
    return await message.reply("Напиши число или напиши /cancel")

# Сохраняем ставку
@dp.message_handler(lambda message: message.text.isdigit(), state=Form.price)
async def stack(message: Message, state: FSMContext):
    async with state.proxy() as data:
        data['price'] = int(message.text)
    ids = data['job'][0]
    url1 = data['job'][1]
    url2 = data['job'][2]
    name_df = pd.read_sql("""select name from main_base_coins where id = %(ids)s""", con=conn_pg, params={'ids': ids})
    name1 = name_df.iloc[0][0]
    while(True):
        try:
            photos1 = vk_api.photos.edit(
                        owner_id=owner_id, 
                        photo_id=url1,
                        caption=f"{name1} Цена {int(message.text)} руб. По всем вопросам в Л.С."
                        )  # Получаем старую цену
            photos2 = vk_api.photos.edit(
                        owner_id=owner_id, 
                        photo_id=url2,
                        caption=f"{name1} Цена {int(message.text)} руб. По всем вопросам в Л.С."
                        )  # Получаем старую цену               
            break
        except exp.Captcha:
            await message.reply("Не получилось. Пришла капча")
            await state.finish()      
        except:
            time.sleep(2)
            continue
    #меняем цену в таблице
    p = message.text
    query = """update main_base_coins as f
            set price = """+p+"""::integer
            where f.id = """+str(ids)+"""::integer"""
    with conn_pg.begin() as conn:
        conn.execute(query)
    await state.finish()
    await message.reply("Готово")

@dp.message_handler(lambda message: message.text.isdigit(), state=Form.selfprice)
async def stack(message: Message, state: FSMContext):
    async with state.proxy() as data:
        data['selfprice'] = int(message.text)
    ids = data['job'][0]
    url1 = data['job'][1]
    url2 = data['job'][2]
    name_df = pd.read_sql("""select comment_id1, comment_id2 from main_base_coins where id = %(ids)s""", con=conn_pg, params={'ids': ids})
    com_id1 = name_df.iloc[0][0]
    com_id2 = name_df.iloc[0][1]
    print(com_id1)
    while(True):
        try:
            photos1 = vk_api.photos.deleteComment(
                        owner_id=owner_id, 
                        comment_id=com_id1,
                        )  # Удаляем коммент   
            break            
        # except exp.Captcha:
        #     await message.reply("Не получилось. Пришла капча")
        #     await state.finish() 
        except:
            time.sleep(2)
            continue
    while(True):
        try:
            photos2 = vk_api.photos.deleteComment(
                        owner_id=owner_id, 
                        comment_id=com_id2,
                        )  # Удаляем коммент
            break            
        # except exp.Captcha:
        #     await message.reply("Не получилось. Пришла капча")
        #     await state.finish() 
        except:
            time.sleep(2)
            continue
    while(True):
        try:
            id_comment_1 = vk_api.photos.createComment(
                        owner_id=owner_id,
                        photo_id=url1,
                        message=message.text
                        )  # Добавляем коммент  
            break
        except exp.Captcha:
            await message.reply("Не получилось. Пришла капча")
            await state.finish()          
        except:
            time.sleep(2)
            continue
    time.sleep(2)
    while(True):
        try:
            id_comment_2 = vk_api.photos.createComment(
                        owner_id=owner_id,
                        photo_id=url2,
                        message=message.text
                        )  # Добавляем коммент     
            break
        except exp.Captcha:
            await message.reply("Не получилось. Пришла капча")
            await state.finish()        
        except:
            time.sleep(2)
            continue  
    #меняем цену в таблице
    p = message.text
    query = """update main_base_coins as f
            set selfprice = """+p+"""::integer, comment_id1 = """+str(id_comment_1)+"""::integer,
            comment_id2 = """+str(id_comment_2)+"""::integer
            where f.id = """+str(ids)+"""::integer"""
    with conn_pg.begin() as conn:
        conn.execute(query)
    await state.finish()
    await message.reply("Готово")

@dp.message_handler(content_types=['text'], state=Form.owner)
async def stack(message: Message, state: FSMContext):
    async with state.proxy() as data:
        data['owner'] = message.text
    #достаем id
    ids = data['job'][0]
    #меняем владельца в таблице
    p = message.text
    query = """update main_base_coins as f
            set owner = '"""+p+"""'
            where f.id = """+str(ids)+"""::integer"""
    with conn_pg.begin() as conn:
        conn.execute(query)
    await state.finish()
    await message.reply("Готово")

if __name__ == '__main__':
    #закидываем постоянные задачи для переноса фоток и бэкапа
    print(4)
    loop = asyncio.get_event_loop()
    loop.create_task(scheduled_backup_file(28800)) # поставим 10 секунд, в качестве теста
    loop.create_task(scheduled_replace_photo(3600))
    # print(5)
    executor.start_polling(dp, skip_updates=True)
    # print(6)

# def main1():
#     #закидываем постоянные задачи для переноса фоток и бэкапа
#     print(4)
#     loop = asyncio.get_event_loop()
#     loop.create_task(scheduled_backup_file(28800)) # поставим 10 секунд, в качестве теста
#     loop.create_task(scheduled_replace_photo(3600))
#     # print(5)
#     executor.start_polling(dp, skip_updates=True)