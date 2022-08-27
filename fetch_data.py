import re
import requests
from bs4 import BeautifulSoup 

import mysql.connector
mydb = mysql.connector.connect(
  host="127.0.0.1",
  user="root",
  password="",
  database="hiring"
)#وصل شدن به دیتا بیس مورد نظر برای اضافه کردن اگهی های سایت

from apscheduler.schedulers.blocking import BlockingScheduler# با استفاده از این لایبرری میتونیم هر 1 ساعت این اسکریپت رو ران کنیم

def fetch_data_1hour():
    counter = 0 
    for x in range(1,21):
        counter = counter + 1
        req = requests.get('https://karboom.io/jobs/programming-and-software?job_category_id[0]=11&page=%i' % counter)
        res = BeautifulSoup(req.text , 'html.parser')
        print (counter)
        projects = res.find_all('div',attrs={'class':'box-intro js-job-item flex-col-between'})
# با استفاده از متغیر کانتر در 20 صفحه سایت مورد نظر با حلقه میگردیم و به ترتیب اگهی های هر صفحه را جدا میکنیم 
        for x in projects: #اطلاعات هر اگهی درون یک لینک بود پس مجبور نوشتن یک حلقه فور دیگر برای استخراج اطلاعات هر اگهی شدم
            infos = list()
            x= str(x)
            x = re.search(r'data-url.+\>' , x)
            x = str(x)
            x = re.sub(r'^.+url..' ,'' ,x)
            x = re.sub(r'\"\>' ,'' ,x) 
            every_item = requests.get(x)#لینک خالص هر اگهی رو دراوردم تا به هرکدام درخواست بزنم و  اطلاعات اگهی را با وب اسکرپینگ جدا کنم 
            if str(every_item) == '<Response [200]>':
                pages_information = BeautifulSoup(every_item.text , 'html.parser')#حالا تکست بدست امده از درخواست را به اچ تی ام ال پارسر تبدیل میکنم
                topic = pages_information.find('div' , attrs = {'class':'company-title col-md-6 col-lg-6'})#قسمت مورد نظر را جدا میکنیم
                topic = str(topic.text).split('\n') #با فانکشن تکست و اسپلیت قسمت مورد نظر را تمیز میکنیم 
                for x in topic:#رد کردن تکست از فیلتر چون فانکشن استریپ کار نمیکرد من یه حلقه نوشتم که اسپیس های اضافی را بگیرد
                    if x == '':
                        continue
                    else:
                        infos.append(x)#اطلااعات قسمت مورد نظرم را پس از تمیز کردن و رد کردن از فیلتر حلقه بالا به یک لیست اضافه کردم 
                    #تا بعد هر کدام از اندکس های لیست را به ترتیب در متغیر مورد نظرم بریزم و جدا کنم
                #سایتی که انتخاب کردم به شدت بی نظم هست و هر اگهی یه جوره بعضی اگهی ها اصلا اسم شهرو نزدن پس مجبور به کنترل ورودی ها شدم که مشکلی پیش نیاد
            
                try: 
                    job_title = infos[0]
                    company_name=infos[1]
                    city = infos[2]
                    payment = infos[3] 
                except:
                    continue
            # به هر دلیلی ممکن بود بعضی اگهی ها یک سری اطلاعات رو نداشته باشند در صورتی که قبلیا همه اون اطلاعات رو داشتند پس برای کنترل ارور
            # یک تصمیم گرفتم ترای بنویسم که اگر اگهی اطلاعاتی نداشت و ناقص بود نادیده گرفته بشه
                else:
                    job_title = infos[0]
                    company_name=infos[1]
                    city = infos[2]
                    payment = infos[3] #اطلاعات مورد نظرم از سایت را در متغیر ها ریختم 

                topic_2 = pages_information.find('div' , attrs = {'class':'row m-0 display-flex'})
                a = topic_2.text.split('\n')
                lst = [x for x in a if x != '']
                if 'Type of cooperation' in lst or 'نوع همکاری' in lst:
                    cooperation_type = lst[1] 
                else:
                    cooperation_type = ''
                if city == 'توافقی':
                    city = 'unknown'
                else:
                    pass#یکی از پارامتر هام از بقیه جدا بود مجبور به جدا کردنش شدم و دوباره به تکست درش اوردم و ریختم درون متغیر 

                mytuple = (job_title,company_name,city,payment,cooperation_type)

                mycursor = mydb.cursor()
                mycursor.execute('SELECT * FROM jobs2')
                mydata = mycursor.fetchall() #با این فانکشن و تاپلی که درست کردم چک میکنم که دیتا تکراری توی دیتا بیس ذخیره نشه
            #اطلاعات به صورت تاپل درون مای دیتا هستن منم با تاپل چک میکنم که هر اگهی ذخیره شده یا نه
                if mytuple in mydata:
                    pass
                else:
                    mycursor2 = mydb.cursor()
                    mycursor2.execute('INSERT INTO jobs2 VALUES (\'%s\' , \'%s\' , \'%s\', \'%s\',\'%s\')'
                    %(job_title,company_name,city,payment,cooperation_type))
                    mydb.commit() #و اگر اگهی ذخیره نشده بود و جدید بود ان را در دیتا بیس ذحیره میکنم

fetch_data_1hour()#برنامه اول یک بار با استفاده از این کال فانکشن اجرا میشود  (برای تست کردن مصحح عزیز)

scheduler = BlockingScheduler() #و پس از اجرای اول هر 3 ساعت یک بار دوباره اجرا میشود و اجرای اولی فقط برای تست مصحح بود
scheduler.add_job(fetch_data_1hour, 'interval', hours=3)# این کار با استفاده از لایبرری ایمپورت شده در بالا صورت گرفت
scheduler.start()

def search_jobtitle():
    search_through_jobtitle = input('search through jobtitles : ')
    mycursor3 = mydb.cursor()
    mycursor3.execute("SELECT * FROM jobs2 WHERE jobtitle LIKE '%{}%' ".format(search_through_jobtitle))
    myresult = mycursor3.fetchall()
    return myresult
#این دو فانکشن برای سرچ کردن درون دیتا بیس هستن و با خواندن دیتا از دیتا بیس انجام میشوند و با کلمه کلیدی لایک و کلمه های شبیه به کلمه ورودی را میابم
#  مثل خود سایت دو نوع سرچ رو مد نطر گرفتم یکی سرچ بین موضوعات شغلی و یک سرچ بین شهر های اگهی ها
def search_city():
    search_through_cities = input('search through cities : ')
    mycursor4 = mydb.cursor()
    mycursor4.execute("SELECT * FROM jobs2 WHERE jobtitle LIKE '%{}%' ".format(search_through_cities))
    myresult2 = mycursor4.fetchall()
    return myresult2
