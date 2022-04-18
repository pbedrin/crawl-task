import asyncio, json, locale
from datetime import datetime
from playwright.async_api import async_playwright

locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')

class news:
    def __init__(self, url, title, date, text, tags, author, img):
        self.url = url
        self.title = title
        self.date = date
        self.text = text
        self.tags = tags
        self.author = author
        self.img = img

# Parsing of different date formats for prememinister.kz

async def kz_date_parse(date):
    for old, new in [
        ('Январь', 'янв'),('Февраль', 'фев'), ('Март', 'мар'),
        ('Апрель', 'апр'),('Май', 'май'), ('Июнь', 'июн'),
        ('Июль', 'июл'),('Август', 'авг'), ('Сентябрь', 'сен'),
        ('Октябрь', 'окт'),('Ноябрь', 'ноя'), ('Декабрь', 'дек')
    ]: date = date.replace(old, new)
    return datetime.strptime(date, "%d %b %Y, %H:%M")
    
async def read_news(link):
    async with async_playwright() as p:
        browser = await p.webkit.launch()
        page = await browser.new_page()

        # Opening news. If page loading error occurs, try again

        try:
            if "primeminister.kz" in link:
                # After a while, primeminister.kz blocks access and asks for a captcha, so:
                await page.goto('http://api.scraperapi.com?api_key=eda40066b9c3f3bf23f5426f91c073b1&url=' + link, timeout=0)
            else:
                await page.goto(link, timeout=0)
        except:
            print("Error: open", link)
            return (await read_news(link))
        
        # Read news from https://paodkb.org/

        if "paodkb.org/events" in link:
            title = (await page.locator('xpath=//h1[@class="content__title content__title--h2"]').text_content()).strip()
        
            date = await page.locator('xpath=//div[@class="content__date"]').text_content()
            date = datetime.strptime(date, "%d %B %Y").strftime("%Y-%m-%d %H:%M:%S")
        
            text = ''
            text_blocks = page.locator('xpath=//div[@class="wysiwyg js-wysiwyg"]/p')
            text_blocks_count = await text_blocks.count()
            for i in range(text_blocks_count):
                text += (await text_blocks.nth(i).text_content()).strip() + " "
            text = text.strip()

            imgs = []
            if await page.locator('xpath=//div[@class="wysiwyg js-wysiwyg"]/figure/img').count() > 0:
                imgs_block = page.locator('xpath=//div[@class="wysiwyg js-wysiwyg"]/figure/img')
                img_count = await imgs_block.count()
                for i in range(img_count):
                    imgs.append(await imgs_block.nth(i).get_attribute('src'))

            tags = []
            author = ''

            res_obj = news(link, title, date, text, tags, author, imgs)
        
        # Read news from https://uz.sputniknews.ru/

        elif "uz.sputniknews.ru" in link:
            title = ''
            if await page.locator('xpath=//h1[@class="article__title"]').count() > 0:
                title = (await page.locator('xpath=//h1[@class="article__title"]').text_content()).strip()

            date = ''
            if await page.locator('xpath=//div[@class="article__info-date"]/a').count() > 0:
                date_unix = await page.locator('xpath=//div[@class="article__info-date"]/a').get_attribute('data-unixtime')
                date = datetime.fromtimestamp(int(date_unix)).strftime("%Y-%m-%d %H:%M:%S")

            text = ''
            if await page.locator('xpath=//div[@class="article__announce-text"]').count() > 0:
                text = await page.locator('xpath=//div[@class="article__announce-text"]').text_content() + "\n"
            text_blocks = page.locator('xpath=//div[@class="article__body"]/div[@class="article__block"]')
            text_blocks_count = await text_blocks.count()
            for i in range(text_blocks_count):
                data_type = await text_blocks.nth(i).get_attribute("data-type")
                if data_type != "media" and  data_type != "article":
                    text += await text_blocks.nth(i).text_content() + " "

            imgs = []
            if await page.locator('xpath=//div[@class="photoview__open"]/img').count() > 0:
                imgs_block = page.locator('xpath=//div[@class="article "]//div[@class="photoview__open"]/img')
                img_count = await imgs_block.count()
                for i in range(img_count):
                    if await imgs_block.nth(i).get_attribute('data-src'):
                        imgs.append(await imgs_block.nth(i).get_attribute('data-src'))
                    elif await imgs_block.nth(i).get_attribute('src'):
                        imgs.append(await imgs_block.nth(i).get_attribute('src'))
            if await page.locator('xpath=//div[@class="article__infographics_variant" and @data-type="desktop"]/img').count() > 0:
                imgs_block = page.locator('xpath=//div[@class="article "]//div[@class="article__infographics_variant" and @data-type="desktop"]/img')
                img_count = await imgs_block.count()
                for i in range(img_count):
                    imgs.append(await imgs_block.nth(i).get_attribute('src'))
            
            tags = []
            if await page.locator('xpath=//ul[@class="tags m-noButton m-mb20"]/li').count() > 0:
                tags_block = page.locator('xpath=//ul[@class="tags m-noButton m-mb20"]/li')
                for i in range(await tags_block.count()):
                    tags.append(await tags_block.nth(i).text_content())

            author = []
            authors_block = page.locator('xpath=//div[@class="article__author-name"]')
            authors_block_count = await authors_block.count()
            for i in range(authors_block_count):
                author.append(await authors_block.nth(i).text_content())
            
            res_obj = news(link, title, date, text, tags, author, imgs)
        
        # Read news from https://primeminister.kz/ru

        elif "primeminister.kz" in link:
            title = (await page.locator('xpath=//h1[@class="articles__title"]').text_content()).strip()

            date = await page.locator('xpath=//p[@class="article__date"]').text_content()
            date = (await kz_date_parse(date)).strftime("%Y-%m-%d %H:%M:%S")
            
            text = ''
            text_blocks = page.locator('xpath=//div[@class="articles__container"][2]/*')
            text_blocks_count = await text_blocks.count()
            for i in range(text_blocks_count):
                # Exclude Telegram ad
                data_type = await text_blocks.nth(i).get_attribute("class")
                if data_type != "articles__container articles__container-bottom":
                    text += await text_blocks.nth(i).text_content() + " "
            text = text.strip()

            imgs = []
            if await page.locator('xpath=//div[@class="articles__container"][2]//img').count() > 0:
                imgs_blocks = page.locator('xpath=//div[@class="articles__container"][2]//img')
                imgs_blocks_count = await imgs_blocks.count()
                for i in range(imgs_blocks_count):
                    imgs.append(await imgs_blocks.nth(i).get_attribute('src'))
            
            tags = []
            author = ''

            res_obj = news(link, title, date, text, tags, author, imgs)

        await browser.close()
        
        return res_obj