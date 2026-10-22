# Built-in modules
import sys
import json
# Third-party modules
import pandas as pd
import requests
from sqlalchemy import text
from tg_bot import *

# Custom modules
import os
if os.environ.get("WORK_DIR"):  # папка с db_connection / bx24 / tg_bot
    sys.path.append(os.environ["WORK_DIR"])
from db_connection import engine
from bx24 import *

descriptors = {
    "Данные для строки 5а": "ufCrm16_1694860718",
    "Данные по авто": "ufCrm16_1695318092",
    "Дата вывоза": "ufCrm16_1694775200",
    "Дата комментария ФЭО": "ufCrm16_1695986031",
    "Дата подписания акта ФГИС": "ufCrm16_1695888486",
    "Дополнительные ограничения транспорт:": "ufCrm16_1694701185",
    "Код отхода ФККО": "ufCrm16_1694700893",
    "Количество грузовых мест": "ufCrm16_1695987759690",
    "Комментарий ФЭО": "ufCrm16_1695892069",
    "Марка машины": "ufCrm16_1695986651036",
    "Масса, т (Брутто)": "ufCrm16_1695986110366",
    "Метод определения массы груза": "ufCrm16_1695987668025",
    "Наименование отхода ФККО": "ufCrm16_1694700927",
    "Наша организация": "ufCrm16_1696418503",
    "Номер контракта транспортировщика": "ufCrm16_1694701134",
    "Отходообразователь E-mail": "ufCrm16_1694701028",
    "Отходообразователь Конт. лицо": "ufCrm16_1694701083",
    "Отходообразователь моб. телефон": "ufCrm16_1694701069",
    "Отходообразователь телефон": "ufCrm16_1694701054",
    "Перевозчик": "ufCrm16_1695318035",
    "Расстояние": "ufCrm16_1694701203",
    "Регион организации переработчика": "ufCrm16_1694701260",
    "Статус заказа во ФГИС": "ufCrm16_1694771942",
    "Стоимость вывоза": "ufCrm16_1697970257",
    "тех_id_товара": "ufCrm16_1696598082",
    "тех_письмо_было_отправлено": "ufCrm16_1698324128",
    "Упаковочные материалы (Наименование тары - Количество)": "ufCrm16_1695985825063",
    "Грузополучатель_привязка_к_компании": "ufCrm16_1696418694",
    "Желаемая дата вывоза с":"ufCrm16_1700473566",
    "Желаемая дата вывоза до":"ufCrm16_1700473604",
}

# Configuration
entityTypeId = 137
logging.basicConfig(level=logging.ERROR)

#запрашиваем все проекты ФЭО

data = b.get_all('crm.item.list', params={'entityTypeId': 137})
bx_feo = pd.DataFrame(data)

bot_q = (
    """SELECT
    *
from bids_order_transportation bot
         """
)
bot = pd.read_sql(bot_q, engine)

# Define the update logic
def get_company_id(name_short):
    if name_short == 'ООО "СТАНДАРТ"':
        return 235
    elif name_short == 'ООО "ПУЛЬС"':
        return 173
    return None  # Or a default value

# Update the 'bot' DataFrame with the company IDs
bot['winner_respond_transportation_org_info_name_short'] = bot['winner_respond_transportation_org_info_name_short'].apply(get_company_id)


columns = ['id',
           'stageId',
           'ufCrm16_1694076789',  # bid_id
           'ufCrm16_1694076828',  # order_transportation_id
           'ufCrm16_1696418503',
           'ufCrm16_1700473604', # id товара
           ]

#клеим заказы из таблицы с битрикс фэо по bid_id

bx_feo = bx_feo[columns]
result = bot.merge(bx_feo, left_on='id', right_on='ufCrm16_1694076789', suffixes=('_bot', '_bx'))
result.rename(columns={'id_bx': 'id'}, inplace=True)



# Here we define the update logic


#result = result[pd.isnull(result['ufCrm16_1700473604'])] #фильтруем, если надо по пустому полю

#result.to_excel('update_feo.xlsx')

# Define the fields to update from each row
def fields_from_row(row):
    return {
        'ufCrm16_1696418503': row['winner_respond_transportation_org_info_name_short'],
        'ufCrm16_1700473566': row['date_unloading_from'],
        'ufCrm16_1700473604': row['date_unloading_to'],
        'ufCrm16_1694700876': row['mass'],
        'ufCrm16_1695986110366': row['mass_gross'],
        # Add other fields to be updated if necessary
    }

# Use the updated 'bot' DataFrame to update Bitrix24
updated_items = update_items_in_bx24_from_df(result, entityTypeId, fields_from_row)
count_updated_item = len(updated_items)

# Output the result
message = f"Updated IDs: {count_updated_item}"
print(message)
