# backpack_liquidation_limit
Это Python-бот для ловли ликвидаций на рынке SOL_USDC_PERP. Он автоматически открывает шорт и лонг на суб-аккаунтах, ставит тейк-профиты по ценам ликвидации и оптимизирует вывод средств, чтобы минимизировать комиссии и затраты, обеспечивая эффективный заработок на ликвидациях.

## Установка

1. Клонируйте репозиторий:  
   ```bash
   git clone https://github.com/savplux/backpack_liquidation_limit
   cd backpack_liquidation_limit
   
2. Создайте виртуальное окружение и установите зависимости:
python -m venv venv
source venv/bin/activate   # Linux/MacOS
venv\Scripts\activate      # Windows
pip install -r requirements.txt

3. Настройка

Заполните # config.yaml поля своими API-ключами и параметрами (пример ниже).

4. Запуск
python backpack_liquidation_bot.py
Бот создаст потоки для каждой пары, автоматически разнесёт их старт до pair_start_delay_max и будет работать в непрерывном цикле.
