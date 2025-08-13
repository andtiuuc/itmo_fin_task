
import psycopg

def connect_to_db():
    #Устанавливает соединение с базой данных
    try:
        conn = psycopg.connect(
            dbname="farmers_markets",
            user=input("\nВведите имя пользователя PostgreSQL: "),
            password=input("Введите пароль пользователя PostgreSQL: "),
            host="localhost"
        )
        return conn
    except psycopg.Error as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

def search_by_city(conn):
    #Поиск рынков по городу (частичное совпадение)
    city = input("Введите город для поиска (Alexandria, Jersey City, Beaver Dam): ").strip()
    
    query = """
    SELECT m.MarketID, m.Name, m.City, m.State
    FROM Markets m
    WHERE m.City ILIKE %s
    ORDER BY m.MarketID;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (f"%{city}%",))
        results = cur.fetchall()
    
    print(f"\nНайдено {len(results)} рынков в городах, содержащих '{city}':")
    print("- FMID, Market Name, City, State")
    for market in results:
        print(f"- {market[0]}, {market[1]}, {market[2]}, {market[3]}")

def search_by_state(conn):
    #Поиск рынков по штату
    state = input("Введите штат для поиска (Minnesota, Alabama, Idaho): ").strip()
    
    query = """
    SELECT m.MarketID, m.Name, m.City, m.State
    FROM Markets m
    WHERE m.State = %s
    ORDER BY m.MarketID;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (state,))
        results = cur.fetchall()
    
    print(f"\nНайдено {len(results)} рынков в штате {state}:")
    print("- FMID, Market Name, City, State")
    for market in results:
        print(f"- {market[0]}, {market[1]}, {market[2]}, {market[3]}")

def search_by_zip(conn):
    #Поиск рынков по почтовому индексу
    zip_code = input("Введите почтовый индекс (53818, 37915, 48111): ").strip()
    
    query = """
    SELECT m.MarketID, m.Name, m.Street, m.City, m.State
    FROM Markets m
    WHERE m.Zip = %s
    ORDER BY m.Name;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (zip_code,))
        results = cur.fetchall()
    
    print(f"\nНайдено {len(results)} рынков с индексом {zip_code}:")
    print("- FMID, Market Name (адрес: Street, City, State)")
    for market in results:
        print(f"- {market[0]}, {market[1]}, (адрес: {market[2]}, {market[3]}, {market[4]})")

def show_market_details(conn):
    #Показывает подробную информацию о рынке (поиск по названию или ID)
    search_input = input("Введите название рынка (полное или частичное) или его ID: ").strip()
    
    # Определяем, ищем ли мы по ID или по названию
    try:
        market_id = int(search_input)
        # Поиск по ID
        query = """
        SELECT m.MarketID, m.Name, m.Website, m.Street, m.City, 
               m.County, m.State, m.Zip, m.Location, m.Credit,
               m.x, m.y, m.UpdateTime
        FROM Markets m
        WHERE m.MarketID = %s;
        """
        params = (market_id,)
    except ValueError:
        # Поиск по названию
        query = """
        SELECT m.MarketID, m.Name, m.Website, m.Street, m.City, 
               m.County, m.State, m.Zip, m.Location, m.Credit,
               m.x, m.y, m.UpdateTime
        FROM Markets m
        WHERE LOWER(m.Name) LIKE LOWER(%s)
        ORDER BY m.Name;
        """
        params = (f"%{search_input}%",)
    
    with conn.cursor() as cur:
        cur.execute(query, params)
        results = cur.fetchall()
    
    if not results:
        print("Рынок не найден")
        return
    
    # Если по ID, сразу переходим к деталям
    if len(results) == 1 and 'market_id' in locals():
        market_id = results[0][0]
    else:
        print("\nНайдены следующие рынки:")
        for i, market in enumerate(results, 1):
            print(f"""{i}. 
    ID: {market[0]}
    Рынок: {market[1]}
    Адрес: {market[3]}, {market[4]}, {market[6]} {market[7]}
            """)
        
        try:
            choice = int(input("Введите номер рынка из списка (1, 2, 3, ...): ")) - 1
            if choice < 0 or choice >= len(results):
                print("Неверный выбор")
                return
            market_id = results[choice][0]
        except ValueError:
            print("Пожалуйста, введите число")
            return
    
    # Получаем детальную информацию (как в предыдущей версии)
    market_query = """
    SELECT m.MarketID, m.Name, m.Website, m.Street, m.City, 
           m.County, m.State, m.Zip, m.Location, m.Credit,
           m.x, m.y, m.UpdateTime
    FROM Markets m
    WHERE m.MarketID = %s;
    """
    
    seasons_query = """
    SELECT s.SeasonNumber, s.StartDate, s.EndDate
    FROM MarketSeasons s
    WHERE s.MarketID = %s
    ORDER BY s.SeasonNumber;
    """
    
    hours_query = """
    SELECT h.DayOfWeek, h.OpenTime, h.CloseTime
    FROM MarketHours h
    WHERE h.MarketID = %s
    ORDER BY 
        CASE h.DayOfWeek
            WHEN 'Monday' THEN 1
            WHEN 'Tuesday' THEN 2
            WHEN 'Wednesday' THEN 3
            WHEN 'Thursday' THEN 4
            WHEN 'Friday' THEN 5
            WHEN 'Saturday' THEN 6
            WHEN 'Sunday' THEN 7
            ELSE 8
        END,
        h.OpenTime;
    """
    
    products_query = """
    SELECT p.ProductType
    FROM MarketProducts p
    WHERE p.MarketID = %s AND p.Available = 'Y'
    ORDER BY p.ProductType;
    """
    
    payments_query = """
    SELECT p.PaymentType
    FROM MarketPayments p
    WHERE p.MarketID = %s AND p.Accepted = 'Y'
    ORDER BY p.PaymentType;
    """
    
    web_query = """
    SELECT w.WebsiteType, w.URL
    FROM MarketWebPresence w
    WHERE w.MarketID = %s
    ORDER BY w.WebsiteType;
    """
    
    with conn.cursor() as cur:

        cur.execute(market_query, (market_id,))
        market_info = cur.fetchone()
        
        cur.execute(seasons_query, (market_id,))
        seasons = cur.fetchall()
        
        cur.execute(hours_query, (market_id,))
        hours = cur.fetchall()
        
        cur.execute(products_query, (market_id,))
        available_products = [row[0] for row in cur.fetchall()]
        
        #таблица криво заполнилась, credit не вошло в marketpayments
        """
        cur.execute(payments_query, (market_id,))
        accepted_payments = [row[0] for row in cur.fetchall()]
        """
        
        cur.execute(web_query, (market_id,))
        web_presence = cur.fetchall()
    
    # Вывод информации (с ID в заголовке)
    print("\nИнформация о рынке:")
    
    print(f"\nFMID: {market_info[0]}")
    print(f"Название: {market_info[1]}")
    print(f"Вебсайт: {market_info[2] or 'не указан'}")
    print(f"Адрес: {market_info[3]}, {market_info[4]}, {market_info[6]} {market_info[7]}")
    print(f"Округ: {market_info[5] or 'не указан'}")
    print(f"Координаты: ({market_info[10]}, {market_info[11]})")
    print(f"Последнее обновление: {market_info[12]}")
    
    print("\nСезоны работы:")
    if seasons:
        for season in seasons:
            print(f"- Сезон {season[0]}: с {season[1]} по {season[2]}")
    else:
        print("Информация о сезонах отсутствует")
    
    print("\nЧасы работы:")
    if hours:
        for day in hours:
            print(f"- {day[0]}: с {day[1]} до {day[2]}")
    else:
        print("Информация о часах работы отсутствует")
    
    print("\nДоступные продукты:")
    if available_products:
        for product in available_products:
            print(f"- {product}")
    else:
        print("Нет доступных продуктов")
    
    #отключено, т.к. в таблице marketpayments загрузились не все способы оплаты
    """
    print("\nСпособы оплаты:")
    if accepted_payments:
        for payment in accepted_payments:
            print(f"- {payment}")
    else:
        print("Нет доступных способов оплаты")
    """
    
    print("\nВеб-ресурсы:")
    if web_presence:
        for web in web_presence:
            print(f"- {web[0]}: {web[1]}")
    else:
        print("Веб-ресурсы не указаны")

def sort_by_fmid(conn):
    """Сортировка рынков по ID"""
    query = """
    SELECT m.MarketID, m.Name, m.City, m.State
    FROM Markets m
    ORDER BY m.MarketID
    LIMIT 50;
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
    
    print("\nПервые 50 рынков, отсортированных по ID:")
    print("- FMID - Market Name, City, State")
    for market in results:
        print(f"- {market[0]} - {market[1]}, {market[2]}, {market[3]}")

def sort_by_name(conn):
    #Сортировка рынков по названию
    query = """
    SELECT m.Name, m.City, m.State, m.MarketID
    FROM Markets m
    ORDER BY m.Name
    LIMIT 50;
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
    
    print("\nПервые 50 рынков, отсортированных по названию:")
    print("- Market Name - FMID, City, State")
    for market in results:
        print(f"- {market[0]} - {market[3]}, {market[1]}, {market[2]}")

def sort_by_city(conn):
    #Сортировка рынков по городу
    query = """
    SELECT m.City, m.MarketID, m.Name, m.State
    FROM Markets m
    ORDER BY m.City, m.Name
    LIMIT 50;
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
    
    print("\nПервые 50 рынков, отсортированных по городу:")
    print("- City - FMID, Market Name, State")
    for market in results:
        print(f"- {market[0]} - {market[1]}, {market[2]}, {market[3]}")

def sort_by_state(conn):
    #Сортировка рынков по штату
    query = """
    SELECT m.State, m.City, m.Name, m.MarketID
    FROM Markets m
    ORDER BY m.State, m.City, m.Name
    LIMIT 50;
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
    
    print("\nПервые 50 рынков, отсортированных по штату:")
    print("- State - FMID, Market Name, City")
    for market in results:
        print(f"- {market[0]} - {market[3]}, {market[2]}, {market[1]}")


def search_markets_by_season(conn):

    #Поиск рынков, работающих в указанный сезон (использует INNER JOIN)
    #Запрашиваем у пользователя номер сезона и выводим соответствующие рынки

    print("\nПоиск рынков по сезону работы")
    print("Доступные сезоны: 1 (весна), 2 (лето), 3 (осень), 4 (зима)")
    
    try:
        season_number = int(input("Введите номер сезона (1-4): "))
        if season_number < 1 or season_number > 4:
            print("Номер сезона должен быть от 1 до 4")
            return
    except ValueError:
        print("Пожалуйста, введите число от 1 до 4")
        return

    # Запрос с INNER JOIN: выбираем только рынки, у которых есть указанный сезон
    query = """
    SELECT DISTINCT 
        m.MarketID AS "ID рынка",
        m.Name AS "Название",
        m.City AS "Город",
        m.State AS "Штат",
        s.StartDate AS "Начало сезона",
        s.EndDate AS "Конец сезона"
    FROM Markets m
    INNER JOIN MarketSeasons s ON m.MarketID = s.MarketID
    WHERE s.SeasonNumber = %s
    ORDER BY m.State, m.City, m.Name
    LIMIT 50;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (season_number,))
        results = cur.fetchall()
    
    if not results:
        print(f"\nНе найдено рынков, работающих в сезон {season_number}")
        return
    
    print(f"\nДо 50-ти первых рынков, работающих в сезон {season_number}:")
    for market in results:
        print(f"\n- ID: {market[0]}")
        print(f"Название: {market[1]}")
        print(f"Местоположение: {market[2]}, {market[3]}")
        print(f"Период работы: с {market[4]} по {market[5]}")

def show_markets_for_selected_product(conn):
    # Показывает рынки, где доступен выбранный продукт (использует обычный JOIN)
    # Получаем список продуктов
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT ProductType FROM MarketProducts ORDER BY ProductType;")
        products = [row[0] for row in cur.fetchall()]
        
        if not products:
            print("Нет данных о продуктах")
            return
    
    # Показываем меню выбора
    print("\nВыберите продукт:")
    for i, p in enumerate(products, 1):
        print(f"{i}. {p}")
    
    # Выбор продукта
    try:
        choice = int(input("Номер продукта: ")) - 1
        product = products[choice]
    except (ValueError, IndexError):
        print("Неверный выбор")
        return
    
    # Запрос с JOIN: ищем рынки, где доступен указанный продукт
    query = """
    SELECT m.MarketID, m.Name, m.City, m.State
    FROM Markets m
    JOIN MarketProducts p ON m.MarketID = p.MarketID
    WHERE p.ProductType = %s AND p.Available = 'Y'
    ORDER BY m.State, m.City
    LIMIT 50;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (product,))
        markets = cur.fetchall()
        
        print(f"\nДо 50-ти первых рынков с продуктом '{product}':")
        print("- FMID, Market Name, City, State")
        if not markets:
            print("Не найдено")
            return
            
        for m in markets:
            print(f"- {m[0]}, {m[1]}, {m[2]}, {m[3]}")


def main():
    print("Добро пожаловать в анализатор фермерских рынков!")
    
    # Устанавливаем соединение с базой данных
    conn = connect_to_db()
    if not conn:
        return
    
    try:
        while True:
            print("\nВыберите действие:")
            print("1. Подробная информация о рынке")
            print("2. Поиск по штату")
            print("3. Поиск по почтовому индексу")
            print("4. Поиск по городу")
            print("5. Сортировка по ID")
            print("6. Сортировка по имени")
            print("7. Сортировка по городу")
            print("8. Сортировка по штату")
            print("9. Поиск рынков по сезону работы")
            print("10. Поиск рынков по продуктам")
            print("11. Выход")
            
            choice = input("Ваш выбор (1-11): ")
            
            if choice == '1':
                show_market_details(conn)
            elif choice == '2':
                search_by_state(conn)
            elif choice == '3':
                search_by_zip(conn)
            elif choice == '4':
                search_by_city(conn)
            elif choice == '5':
                sort_by_fmid(conn)
            elif choice == '6':
                sort_by_name(conn)
            elif choice == '7':
                sort_by_city(conn)
            elif choice == '8':
                sort_by_state(conn)
            elif choice == '9':
                search_markets_by_season(conn)
            elif choice == '10':
                show_markets_for_selected_product(conn)
            elif choice == '11':
                print("Работа завершена. До свидания!")
                break
            else:
                print("Неверный ввод, попробуйте снова")
    finally:
        # Закрываем соединение при выходе
        conn.close()

if __name__ == "__main__":
    main()