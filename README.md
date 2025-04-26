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

Заполните поля своими API-ключами и параметрами (пример ниже).

# config.yaml
main_account:
  address: "Ваш_адрес"

api:
  key: "MAIN_API_KEY"
  secret: "MAIN_API_SECRET"

symbol: "SOL_USDC_PERP"
initial_deposit: "1"
check_interval: 10

pairs:
  - short_account:
      name: "Short1"
      address: "ADDR1"
      api_key: "SHORT1_KEY"
      api_secret: "SHORT1_SECRET"
    long_account:
      name: "Long1"
      address: "ADDR2"
      api_key: "LONG1_KEY"
      api_secret: "LONG1_SECRET"
  # …другие пары…

limit_order_retries: 60
maker_offset:
  long: 0.000125
  short: 0.000125
limit_order_timeout: 30

take_profit_offset:
  long: 0.05
  short: -0.05

sweep_attempts: 10
general_delay:
  min: 5
  max: 15

leverage: 50
cycle_wait_time: 10
pair_start_delay_max: 120

4. Запуск
python backpack_liquidation_bot.py
Бот создаст потоки для каждой пары, автоматически разнесёт их старт до pair_start_delay_max и будет работать в непрерывном цикле.
