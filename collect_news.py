import asyncio, json, locale
from datetime import datetime
from threading import Thread
from time import time
from nbformat import read
from read_news import read_news
from dateutil.relativedelta import relativedelta
from playwright.async_api import async_playwright

locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')

# Function saves list of news objects (news_list) to file (file_name.json)

async def save_json(news_list, file_name):
    json_string = json.dumps([ob.__dict__ for ob in news_list], ensure_ascii=False, indent=4)
    json_file = open(file_name + ".json", "w")
    json_file.write(json_string)
    json_file.close()

async def collect_news_odkb(date1, date2):
    async with async_playwright() as p:
        browser = await p.webkit.launch()
        page = await browser.new_page()
        obj_list = []

        # News starts from date_start = 2013-07-11
        date_start = datetime(2013, 7, 11)
        if date2 > datetime.today():
            date2 = datetime.today()
        if date2 < date_start:
            # Error: no news for this dates
            return 1
        if date1 < date_start:
            date1 = date_start
        date1_sv = date1

        print("Search for news on ODKB from", date1.strftime("%d.%m.%Y"), "to", date2.strftime("%d.%m.%Y"), file=open('logs_odkb.txt', 'a'))

        # Make list of month-year pairs for crawl
        months_for_scan = []
        while (date1.year < date2.year) or (date1.year == date2.year and date1.month <= date2.month):
            months_for_scan.append(date1)
            print("Added month-year to crawl list: ", date1.strftime("%m.%Y"), file=open('logs_odkb.txt', 'a'))
            date1 = date1 + relativedelta(months=1)
        date1 = date1_sv

        from collections import defaultdict
        date_dict = defaultdict(list)

        # Example of a link to a news list: https://paodkb.org/events?month=MM&year=YYYY&page=PP
        for ym in months_for_scan:
            pages_cnt = 1
            url_for_mon = 'https://paodkb.org/events?month=' + ym.strftime("%-m") + '&year=' + ym.strftime("%Y") + '&page=' + str(pages_cnt)
            print("For", ym.strftime("%m.%Y"), "generated URL =", url_for_mon, file=open('logs_odkb.txt', 'a'))
            await page.goto(url_for_mon)
            
            while await page.locator('xpath=//div[@class="events__content"]/div[@class="events__item"]').count() > 0:
                news_blocks = page.locator('xpath=//div[@class="events__content"]/div[@class="events__item"]')
                news_blocks_count = await news_blocks.count()
                print("On page", page.url, "number of news =", news_blocks_count, file=open('logs_odkb.txt', 'a'))

                for i in range(news_blocks_count):
                    # There can be two <a> in the news card - date and author
                    # If there is no author
                    if await news_blocks.nth(i).locator("xpath=//article/div[2]/div/a").count() == 1:
                        date_elem = news_blocks.nth(i).locator("xpath=//article/div[2]/div/a")
                    # If there is an author
                    else:
                        date_elem = news_blocks.nth(i).locator("xpath=//article/div[2]/div/a[1]")

                    date = (await date_elem.text_content()).strip()
                    date = datetime.strptime(date, "%d %B %Y")
                    
                    if date >= date1 and date <= date2:
                        news_link = await date_elem.get_attribute('href')
                        news_link = "https://paodkb.org" + news_link
                        date_dict[date.strftime("%Y-%m-%d")].append(await read_news(news_link))
                        print(date.strftime("%Y-%m-%d %H:%M:%S"), news_link, file=open('logs_odkb.txt', 'a'))
                
                if date < date1:
                    break
                
                pages_cnt += 1
                url_for_mon = 'https://paodkb.org/events?month=' + ym.strftime("%-m") + '&year=' + ym.strftime("%Y") + '&page=' + str(pages_cnt)
                await page.goto(url_for_mon)

        for key in date_dict:
            await save_json(date_dict[key], "./news_odkb/" + key)
        print('The ODKB crawl is completed.', file=open('logs_odkb.txt', 'a'))
        await browser.close()

async def collect_news_sptnk_task(date1, date2):
    async with async_playwright() as p:
        browser = await p.webkit.launch()
        page = await browser.new_page()

        print("[Task] Search for news on Sputnik from", date1.strftime("%d.%m.%Y"), "to", date2.strftime("%d.%m.%Y"), file=open('logs_sptnk.txt', 'a'))

        # Example of a link to a daily news list: https://uz.sputniknews.ru/YYYYMMDD/
        date_curr = date1
        while date_curr < date2:
            obj_list = []

            url = 'https://uz.sputniknews.ru/' + date_curr.strftime("%Y") + date_curr.strftime("%m") + date_curr.strftime("%d")

            while True:
                try:
                    await page.goto(url, timeout=0)
                except:
                    print("Error loading the news list. Page reload...", 'Date:', date_curr.strftime("%Y-%m-%d"), file=open('logs_sptnk.txt', 'a'))
                    await page.wait_for_timeout(5000)

                    while True:
                        try:
                            await page.reload(timeout=0)
                        except:
                            continue
                        else:
                            break
                    
                    continue
                else:
                    print("The news list is loaded.", 'Date:', date_curr.strftime("%Y-%m-%d"), file=open('logs_sptnk.txt', 'a'))
                    break

            # If there is no news for date_curr, then this is page 404. We move to the next day.
            if await page.locator('xpath=//div[@class="page404"]').count() > 0:
                print('404 is found', url, 'Date:', date_curr.strftime("%Y-%m-%d"), file=open('logs_sptnk.txt', 'a'))
                date_curr += relativedelta(days=1)
                continue

            # If there is news for date_curr
            news_blocks = page.locator('xpath=//div[@class="list list-tag"]/div')
            news_blocks_count = await news_blocks.count()
            print("On page:", page.url, "number of news =", news_blocks_count, file=open('logs_sptnk.txt', 'a'))
            read_cnt = 0

            # If there is more-button OR if not (<=20 news for date_curr)
            while (await page.locator('xpath=//div[@class="list__more"]').count() > 0) or (read_cnt == 0 and news_blocks_count > 0):
                
                date = date_curr
                for i in range(read_cnt, news_blocks_count):
                    read_cnt += 1
                    date_unix = await news_blocks.nth(i).locator('xpath=//div[@class="list__info"]/div[@class="list__date "]').get_attribute('data-unixtime')
                    date = datetime.fromtimestamp(int(date_unix)) + relativedelta(hours=2)

                    if date >= date_curr:
                        news_link = "https://uz.sputniknews.ru" + await news_blocks.nth(i).locator('xpath=//div[@class="list__content"]/a').get_attribute('href')
                        print(i, date.strftime("%Y-%m-%d %H:%M:%S"), news_link, file=open('logs_sptnk.txt', 'a'))
                        obj_list.append(await read_news(news_link))
                    
                    # If the news for date_curr is over, we move to the next day
                    else:
                        break
                
                # If the news for date_curr is over,  we move to the next day
                if date < date_curr:
                    print('Date is end, date=', date.strftime("%Y-%m-%d %H:%M:%S"), 'datecurr=', date_curr.strftime("%Y-%m-%d %H:%M:%S"), file=open('logs_sptnk.txt', 'a'))
                    break

                # If we are here:
                #   - There is a more-button -> news for the date isn't over -> click
                #   - There isn't a more-button -> news for the date is over

                if await page.locator('xpath=//div[@class="list__more"]').count() > 0:
                    await page.locator('xpath=//h1[@class="title"]').scroll_into_view_if_needed()
                    await page.locator('xpath=//div[@class="list__more"]').click()
                    
                    try:
                        await news_blocks.nth(news_blocks_count).wait_for()
                    except:
                        print('Error loading the extended news list. Page reload...', 'Date:', date_curr.strftime("%Y-%m-%d"), file=open('logs_sptnk.txt', 'a'))
                        await page.locator('xpath=//h1[@class="title"]').scroll_into_view_if_needed()
                        await page.wait_for_timeout(5000)

                        while True:
                            try:
                                await page.reload(timeout=0)
                            except:
                                continue
                            else:
                                break
                        
                        news_blocks = page.locator('xpath=//div[@class="list list-tag"]/div')
                        news_blocks_count = await news_blocks.count()
                        print("[Reload] On page:", page.url, "number of news =", news_blocks_count, file=open('logs_sptnk.txt', 'a'))
                        continue

                    news_blocks_count = await news_blocks.count()
                    print("[More] On page:", page.url, "number of news =", news_blocks_count, file=open('logs_sptnk.txt', 'a'))
            
            if obj_list:
                await save_json(obj_list, "./news_sptnk/" + date_curr.strftime("%Y-%m-%d"))

            # If there is no button, then the news for the date_curr is over, move to the next day
            date_curr += relativedelta(days=1)
        
        await browser.close()

async def collect_news_sptnk(date1, date2):
    # News starts from date_start = 2015-06-23
    date_start = datetime(2015, 6, 23)
    if date2 > datetime.today():
        date2 = datetime.today()
    if date2 < date_start:
        # Error: no news for this dates
        return 1
    if date1 < date_start:
        date1 = date_start

    print("Search for news on Sputnik from", date1.strftime("%d.%m.%Y"), "to", date2.strftime("%d.%m.%Y"), file=open('logs_sptnk.txt', 'a'))

    tasks = []
    THREADS = 4
    if (date2 - date1).days < THREADS:
        THREADS = (date2 - date1).days
    diff = (date2  - date1) / THREADS

    for i in range(THREADS):
        d1 = date1 + diff * i
        d1 = d1.replace(hour=0, minute=0, second=0)
        d2 = date1 + diff * (i+1)
        d2 = d2.replace(hour=0, minute=0, second=0)
        print(d1.strftime('%Y-%m-%d %H:%M:%S'), '-', d2.strftime('%Y-%m-%d %H:%M:%S'), file=open('logs_sptnk.txt', 'a'))
        tasks.append(collect_news_sptnk_task(d1, d2))
    
    print(date2.strftime('%Y-%m-%d %H:%M:%S'), '-', (date2 + relativedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'), file=open('logs_sptnk.txt', 'a'))
    tasks.append(collect_news_sptnk_task(date2, date2 + relativedelta(days=1)))

    print('Launching the task list.', file=open('logs_sptnk.txt', 'a'))
    await asyncio.gather(*tasks)
    print('The Sputnik crawl is completed.', file=open('logs_sptnk.txt', 'a'))

async def collect_news_kz_task(date1, date2):
    async with async_playwright() as p:
        browser = await p.webkit.launch()
        page = await browser.new_page()

        print("[Task] Search for news on primeminister.kz from", date1.strftime("%d.%m.%Y"), "to", date2.strftime("%d.%m.%Y"), file=open('logs_kz.txt', 'a'))

        await page.goto('https://primeminister.kz/ru/archive')

        while date1 < date2:

            obj_list = []

            await page.select_option('xpath=//select[@name="day"]', date1.strftime("%d"))
            await page.select_option('xpath=//select[@name="month"]', date1.strftime("%m"))
            await page.select_option('xpath=//select[@name="year"]', str(date1.year))
            await page.locator('button.archive__select.desktop_button').click()
            await page.wait_for_load_state()

            news_blocks = page.locator('xpath=//div[@class="blog__container"]/a')
            news_blocks_count = await news_blocks.count()
            print("On page:", page.url, "number of news =", news_blocks_count, file=open('logs_kz.txt', 'a'))

            for i in range(news_blocks_count):
                date = await news_blocks.nth(i).locator('xpath=//div[@class="article__wrapper"]/div[@class="article__wrapper-top"]/span[@class="article__date"]').text_content()
                date = ' '.join(date.split())
                date = datetime.strptime(date, "%d %b %Y")
                news_link = 'https://primeminister.kz' + await news_blocks.nth(i).get_attribute('href')
                print(date.strftime("%Y-%m-%d %H:%M:%S"), news_link, file=open('logs_kz.txt', 'a'))
                obj_list.append(await read_news(news_link))
            
            if obj_list:
                await save_json(obj_list, "./news_kz/" + date1.strftime("%Y-%m-%d"))
            
            date1 += relativedelta(days=1)
        
        await browser.close()

async def collect_news_kz(date1, date2):
    # News starts from date_start = 2012-01-12
    date_start = datetime(2012, 1, 12)
    if date2 > datetime.today():
        date2 = datetime.today()
    if date2 < date_start:
        # Error: no news for this dates
        return 1
    if date1 < date_start:
        date1 = date_start

    print("Search for news on primeminister.kz from", date1.strftime("%d.%m.%Y"), "to", date2.strftime("%d.%m.%Y"), file=open('logs_kz.txt', 'a'))

    tasks = []
    THREADS = 2
    if (date2 - date1).days < THREADS:
        THREADS = (date2 - date1).days
    diff = (date2  - date1) / THREADS
    
    for i in range(THREADS):
        d1 = date1 + diff * i
        d1 = d1.replace(hour=0, minute=0, second=0)
        d2 = date1 + diff * (i+1)
        d2 = d2.replace(hour=0, minute=0, second=0)
        print(d1.strftime('%Y-%m-%d %H:%M:%S'), '-', d2.strftime('%Y-%m-%d %H:%M:%S '), file=open('logs_kz.txt', 'a'))
        tasks.append(collect_news_kz_task(d1, d2))
    
    print(date2.strftime('%Y-%m-%d %H:%M:%S'), '-', (date2 + relativedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'), file=open('logs_kz.txt', 'a'))
    tasks.append(collect_news_kz_task(date2, date2 + relativedelta(days=1)))

    print('Launching the task list.', file=open('logs_kz.txt', 'a'))
    await asyncio.gather(*tasks)
    print('The Kz crawl is completed.', file=open('logs_kz.txt', 'a'))

async def collect_news(date1, date2):
    # starting a three-thread crawl
    tasks = []
    tasks.append(collect_news_odkb(date1, date2))
    tasks.append(collect_news_sptnk(date1, date2))
    tasks.append(collect_news_kz(date1, date2))
    await asyncio.gather(*tasks)