# Налаштування


Розміщення

* При інсталяції через buildout
    в `./openprocurement.search.buildout/etc`
* При інсталяціїї debian пакета
    в `/etc/search-tenders`


Перелік файлів налаштувань

* [circus.ini](#circus) -  налаштування менеджера процесів
* [ftpsync.ini](#ftpsync) - налаштування FTP синхронізації OCDS файлів
* [gunicorn.conf](#gunicorn) - налаштування формату логу пошукових запитів
* [logrotate.conf](#logrotate) - правила ротації логів
* [search.ini](#search) - основний файл налаштувань
* [/etc/cron.d/search-tenders](#crontab) - періодичні операції


<a name="circus"></a>

## circus.ini

Circus запускає, моніторить роботу і перезапускає в разі аварійного завершення
два основні сервіси:

1. Пошукове HTTP API, що запускається за допомогою gunicorn, приймає і обробляє
пошукові запити, також видає статистику по `GET /heartbeat`
2. Індексуючий демон, що підтримує наповненя індексу в ElasticSearch, веде його
перевірку і переіндексацію за розкладом

Документація [Circus Configuration](http://circus.readthedocs.io/en/latest/for-ops/configuration/)

Приклад налаштувань при інсталяції deb пакета

```ini
[circus]
; сокет для управління через circusctl
endpoint = ipc:///opt/search-tenders/var/circus_endpoint
logoutput = /var/log/search-tenders/circus.log
pidfile = /run/search-tenders.pid


; HTTP API для обробки пошукових запитів
; запускається через gunicorn
; налаштування http сервісу в search.ini
; налаштування gunicorn в accesslog.conf
[watcher:search_server]
cmd = /opt/search-tenders/bin/gunicorn
args = -c /etc/search-tenders/accesslog.conf --paste /etc/search-tenders/search.ini
working_dir = /opt/search-tenders/var
priority = 1
send_hup = True
; розміщення лог-файлів
stdout_stream.class = FileStream
stdout_stream.filename = /var/log/search-tenders/search_error.log
stderr_stream.class = FileStream
stderr_stream.filename = /var/log/search-tenders/search_error.log
; користувач з правами якого працює сервіс
uid = searchtenders
gid = searchtenders


; головний процес індексатора
; налаштування в search.ini
[watcher:index_worker]
cmd = /opt/search-tenders/bin/index_worker
args = /etc/search-tenders/search.ini
working_dir = /opt/search-tenders/var
warmup_delay = 1
priority = 2
copy_env = True
; розміщення лог-файлів
stdout_stream.class = FileStream
stdout_stream.filename = /var/log/search-tenders/index_info.log
stderr_stream.class = FileStream
stderr_stream.filename = /var/log/search-tenders/index_error.log
; користувач з правами якого працює сервіс
uid = searchtenders
gid = searchtenders
```

Типові налаштування рідко змінюються. За потреби є можливість додавати власні
секції `watcher` і таким чином запустити більше одного процеса індексатора з
різними налаштуваннями (окремо індексувати тестові і не тестові тендери)


<a name="ftpsync"></a>

## ftpsync.ini

Використовувався до липня 2016, дозволяв отримувати вигрузку паперових продцедур
з https://ips.vdz.ua/

```ini
[ftpsync]
; ip адреса FTP серверу
host = 10.20.30.40
; користувач і пароль FTP
user = edz_user
passwd = ****
; таймаут на мережеві операції
timeout = 120
; папка куди будуть завантажуватись файли
local_dir = /mnt/di2/ocds
; маска файлів що підпадають під синхронізацію
filematch = ocds-tender-*.json
```


<a name="gunicorn"></a>

## gunicorn.conf

Встановлює розміщення і формат лог файлу пошукових запитів, додає до стандартного
формату час виконання кожного запиту

```ini
accesslog = '/var/log/search_access.log'
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(L)s'
```

Документація по [gunicorn settings](http://docs.gunicorn.org/en/stable/settings.html#access-log-format)

Підключається в `circus.ini`


<a name="logrotate"></a>

## logrotate.conf

Стандартний [logrotate синтаксис](https://linux.die.net/man/5/logrotate.conf)

Винесений окремо для можливості ротації лог-файлів в нестандартний час
(рекомендується в 23:50, щоб не заважати переіндексації)

```
/var/log/search-tenders/*.log
/opt/search-tenders/var/log/*.log {
    su root adm
    weekly
    missingok
    rotate 52
    compress
    delaycompress
    notifempty
    postrotate
        /etc/init.d/search-tenders reloadlogs
    endscript
}
```

Зверніть увагу на перезапуск після ротації за допомогою `reloadlogs`

Стандартний `reload` перехоплюється `systemd` і працює некоректно.


<a name="search"></a>

## search.ini

Основний файл налаштувань, містить в одному місці спільні налаштування для
наступних сервісів:

1. Пошукового HTTP API
2. Індексатора
3. Додаткових утиліт

    - видалення старих індексів `clean_indexes`
    - перевірки наповнення індексів `test_index`
    - перевірки наявності останніх документів `test_search`
    - перервірки швидкості роботи `test_load`
    - перевірки назв організацій `update_orgs`


<a name="server_main"></a>

### Розділ [server:main]

Загальні налаштуванян gunicorn для пошукового HTTP API

Документація див. [gunicorn settings](http://docs.gunicorn.org/en/stable/settings.html)

```ini
[server:main]
proc_name = search_server
pidfile = /run/search_server.pid
use = egg:gunicorn#main
host = 127.0.0.1
port = 8484
workers = 2
worker_class = gevent
max_requests = 10000
timeout = 30
```


<a name="app_main"></a>

### Розділ [app:main]

Загальні налаштування Flask Microframework для пошукового HTTP API

Документація див. [Flask config](http://flask.pocoo.org/docs/0.12/config/)

```ini
[app:main]
name = SearchTenders101
use = egg:openprocurement.search#search_server
timezone = Europe/Kiev
secret_key = 123456-RANDOM-123456
;debug = 1
```

`name` - назва сервісу що потім показується у відповіді `heartbeat`

`secret_key` - ключ доступу до статистики, передається параметром `key`
в `GET /heartbeat?key=123...`

`debug` - додає розшифровку запитів у відповілях Search API


<a name="search_engine"></a>

### Розділ [search_engine]

Основний розділ пошукового сервісу та індексатора, містить налаштування джерел
даних, періодичності переіндексування, режиму Master-Slave, тощо.


<a name="slave_mode"></a>

### Master / Slave Mode

Два індексатора можуть працювати в режимі **master/slave**. В такому режимі
*master* індексує, а *slave* моніторить його роботу і чекає. Якщо за якихось
причин *master* перестає працювати тоді *slave* прокидається і продовжує
індексування. Коли *master* повертається до роботи, *slave* зупиняється.

```ini
[search_engine]
slave_mode = http://10.20.30.40:8484/heartbeat?key=123...
slave_wakeup = 600
```

`slave_mode` - URL для моніторингу *master* ноди. Прописується тільки для
slave ноди, на master має бути закоментовано.

`slave_wakeup` - час простою *master* (секунд) при якому *slave* прокидається,
рекомендується не менше 300 сек.


<a name="elastic"></a>

### ElasticSearch

```ini
index_names  = /opt/search-tenders/var/index_names

elastic_host = 127.0.0.1:9200
;index_elastic_host = 127.0.0.1:9200
;search_elastic_host = 127.0.0.1:9200
elastic_timeout = 300
```

`index_names` - шлях до файлів які визначають поточний стан індексатора, цей
префікс спільний для файлів

 * `index_names.yaml` - список імен індексів (поточних, попередніх, наступних,
 переіндексація яких ще йде)
 * `index_names.heartbeat` - час останньої успішної операції індексування
 * `index_names.lock` - pid-файл що захищає від повторного запуску індексатора

`elastic_host` - підключення до кластеру ElasticSearch

`elastic_timeout` - таймаут операцій ElasticSearch


<a name="orgs"></a>

### EDRPOU database

```ini
orgs_db = /opt/search-tenders/edrpou/edrpou.db
orgs_queue = 1000
```

`orgs_db` - шлях до SQLite бази даних з офіційними назвами юридичних осіб для
розшифровки імен організацій за кодом ЄДРПОУ (див також `tender_decode_orgs`)

`orgs_queue` - розмір черги індексатора підсказок назв організацій



<a name="ocds"></a>

### Індекс "паперові тендери"

Шлях та налаштування джерела OCDS файлів паперових продцедур, які були отримані
за допомогою утиліти `ocds_ftp_sync` див. файл налаштувань `ftpsync.ini`

```ini
;; ocds source
;; ===========
ocds_dir = /opt/search-tenders/var/ocds
;ocds_mask = ocds-tender-*.json
;ocds_skip_until = 2016-01-01
;ocds_index_lang = english,russian,ukrainian
;ocds_reindex = 360,7
;ocds_check = 100000,0
```


<a name="tender"></a>

### Індекс "Тендери ProZorro"

```ini
;; tender source
;; =============
tender_api_key = ""
tender_api_url = https://public.api.openprocurement.org
tender_api_version = 2.3
tender_api_mode = _all_
tender_skip_until = 2016-01-01
tender_skip_after = 2018-04-01
tender_user_agent = search-1
tender_fast_stepsback = 10
tender_fast_client = yes
tender_decode_orgs = yes
;tender_file_cache = /mnt/cache/tenders
;tender_cache_allow = complete,cancelled,unsuccessful
;tender_cache_minage = 15
;tender_index_lang = english,russian,ukrainian
;tender_preload = 10000
;tender_limit = 1000
;tender_reindex = 5,6
;tender_check = 300000,2
;tender_reseteach = 3
;tender_resethour = 22
;tender_skip_test = no
;tender_plugins = module1.plugin1,module2.plugin2
```


`tender_api_key` - ключ доступу до openprocurement.api, при доступі до public
точки може бути пустий, для пошуку запитів в логах ньому корисно вказувати
і'мя сервісу.

`tender_api_url` - URL до openprocurement.api, тільки `protocol://host` без
шляху (без /api/0/tenders)

`tender_api_version` - версія API, якщо не знаєте вкажіть 0

`tender_api_mode` - бажаний список тендерів, можливі значення:

 * (пусто) - тендери без `mode:test`
 * `_all_` - всі тендери разом (test + not_test)
 * `test` - тільки тестові тендери

`tender_skip_until` - не індексувати тендери, зміни в яких відбулсь до цієї
дати, формат ISO 8601

`tender_skip_after` - не індексувати тендери, зміни в яких відбулсь після цієї
дати, формат ISO 8601

`tender_user_agent` - User-Agent в HTTP запитах до openprocurement.api,
корисно для пошуку лог-файлах і відладки запитів

`tender_fast_client` - включити режим 2 курсорів при доступі до API,
_перший_ курсор йде від найстаріших тендерів до нових, _другий_ курсор починає
від найновішого тендера і далі до нових, як тільки вони з'являться.
В такому режимі при повній переіндексації нові тендери будуть індексуватись
паралельно з наповненням індексу старими тендерами.

`tender_fast_stepsback` - кількість кроків назад який робить _другий_ курсор
на старті, один крок дорівнює `tender_limit` тендерів

`tender_decode_orgs` - розшифровувати назви організацій за кодом ЄДРПОУ

`tender_file_cache` - зберігати тендери в файли на диску і використовувати
їх при переіндексації (в параметрі треба вказати шлях) перед використанням
перевірте наявність необхідної кількості вільних `inode` в файловій системі

`tender_cache_allow` - які статуси тендерів дозволені для зберігання в
файловому кеші

`tender_cache_minage` - мінімальний вік тендера якйи дозволено зберігати в
файловому кеші

`tender_index_lang` - використовувати морфологію при пошуку по ключовому
слову за вказанною мовою (english, russian, ukrainian)

`tender_preload` - включити режим предзавантаження списку тендерів, це
заменшує час доступу і кількість помилок 412 при переіндексації або перевірці
індексу

`tender_limit` - розмір списку тендерів за один запит (дозволено до 1000)

`tender_reindex` - два числа через кому, перше - період днів до переіндексації,
друге - перший день тижня коли дозволена переіндексація

Наприклад: tender_reindex=7,5 означає переіндексацію кожні 7 днів з початком
переіндексації в п'ятницю.

`tender_check` - перевіряти індекс за цими критеріями, два числа через кому,
перше - мінімальна кількість документів в індексі, друге - максимальний вік
документа в індексі (днів)

Наприклад: tender_check=100000,2 означає що перевірку пройде індекс в якому
більше 100 тис документів (тендерів) та вік самого молодого не більше 2 днів
(48 годин)

`tender_reseteach` - перез'єднанувати з openprocurement.api кожні N годин
разом автоматичною перевіркою наповнення індексу (за замовчування кожні 3)

`tender_resethour` - година в добі (один раз) коли відбувається перез'єднання
з openprocurement.api та автоматична перевірка наповнення індексу. Еквівалент
tender_reseteach=24 але при цьому дозволяє вказати конкретну годину.

`tender_skip_test` - ігнорувати цей індекс при перевірці утилітою test_search.

`tender_plugins` - список класів плагінів індексатора через кому


<a name="plan"></a>

### Індекс "Плани ProZorro"

```ini
;; plan source
;; ===========
plan_api_key = ""
plan_api_url = https://public.api.openprocurement.org
plan_api_version = 2.3
plan_api_mode = _all_
;plan_skip_until = 2016-01-01
;plan_skip_after = 2018-04-01
;plan_user_agent = search-1
;plan_fast_stepsback = 10
;plan_fast_client = 1
;plan_decode_orgs = 1
;plan_file_cache = /mnt/cache/plans
;plan_cache_minage = 15
;plan_index_lang = english,russian,ukrainian
;plan_preload = 10000
;plan_limit = 1000
;plan_reindex = 12,7
;plan_check = 500000,2
;plan_reseteach = 3
;plan_resethour = 23
;plan_skip_test = no
```

Налаштування аналогічно індексу "[Тендери ProZorro](#tender)"




<a name="auction"></a>

### Індекс "Аукціони ProZorro.Sale"

```ini
;; auction source
;; ==============
auction_api_key = ""
auction_api_url = http://public.api.ea.openprocurement.org
auction_api_version = 2.4
;auction_api_mode = _all_
;auction_index_lang = english,russian,ukrainian
;auction_skip_until = 2016-06-01
;auction_user_agent =
;auction_file_cache = /mnt/cache/auction
;auction_cache_allow = complete,cancelled,unsuccessful
;auction_cache_minage = 15
;auction_preload = 10000
;auction_limit = 1000
;auction_reindex = 5,6
;auction_reseteach = 3
;auction_resethour = 22
;auction_check = 1,10
;auction_skip_test = no
```

Налаштування аналогічно індексу "[Тендери ProZorro](#tender)"


<a name="auction2"></a>

### Індекс "Аукціони2 ProZorro.Sale (майно)"

```ini
;; auction2 source
;; ==============
auction2_api_key = ""
auction2_api_url = http://public.api.ea2.openprocurement.org
;auction2_api_version = 2.3
;auction2_api_mode = _all_
;auction2_...
```

Налаштування аналогічно індексу "[Аукціони Prozorro.Sale](#auction)"


<a name="common"></a>

### Загальні налаштування індексатора

```ini
;; search.ini
;; common settings
;; ===============
force_lower = yes
async_reindex = no
ignore_errors = no
check_on_start = yes
number_of_shards = 6
index_parallel = yes
reindex_loops = 2
query_speed = 100
index_speed = 500
bulk_insert = no
update_wait = 5
start_wait = 5
watchdog = 600
timeout = 30
```


`force_lower` - примусово приводити до нижнього регістру в запитах по ID та ID_like

`async_reindex` - дозволити переіндексацію в окремому процесі без зупинки
поточної індексації

`ignore_errors` - ігнорувати помилки при роботі з openprocurement.api та
elasticsearch

`check_on_start` - проводити перевірку всіх індексів на старті індексатора

`number_of_shards` - кількість шардів для індексів за замовчуванням (крім orgs)

`index_parallel` - дозволити паралельну перевірку повноти одразу декількох
індексів, прискорює старт і вихід на робочий режим

`log_noindex` - логувати тендери які не було проіндексовано за внутрішніми правилами

`reindex_loops` - кількість проходів при повній переіндексації (default: 2)

`optimize_index` - оптимізувати індекс після переіндексації (default: No)

`max_num_segments` - максимальна кількість сегментів для optimize_index (default: 1)

`new_index_diff` - на скільки менше документів може бути в новому індексі (default: 0.999)

`query_speed` - обмежити запити до API такою кількістю запитів на секунду
(приблизно, додає sleep)

`index_speed` - обмежити індексування такою кількістю документів на секунду
(додає sleep)

`bulk_insert` - дозволити пакетне індексування за допомогою
[Bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html)

`update_wait` - пауза між двома циклами оновлення індексу (секунд)

`start_wait` - пауза на старті індексатора (секунд)

`watchdog` - таймер watchdog-а в секундах, якщо процес індексатора "зависне"
більше ніж на цей час - він буде перезапущений (актуально при використанні
плагінів, рекомендоване значення більше 300)

`timeout` - загальний таймаут на мережеві операції


**Рекомендовані значення для індексів менше 100,000 документів**

```ini
force_lower = yes
async_reindex = no
ignore_errors = no
check_on_start = yes
number_of_shards = 2
index_parallel = no
index_speed = 500
bulk_insert = no
update_wait = 10
start_wait = 10
timeout = 30
```


**Рекомендовані значення для індексів більше 100,000 документів**

```ini
force_lower = yes
async_reindex = yes
ignore_errors = no
check_on_start = yes
number_of_shards = 6
index_parallel = yes
index_speed = 5000
bulk_insert = yes
update_wait = 5
start_wait = 5
timeout = 30
```


**Для пісочниць дозволяється**

```ini
ignore_errors = yes
```


<a name="update_orgs"></a>

### Розділ [update_orgs]

```ini
; search.ini
[update_orgs]
pidfile = /opt/search-tenders/var/run/update_orgs.pid
update_days = 30
```

`update_days` - за скільки днів перевіряти організації в тендерах


<a name="loggers"></a>

### Розділ [loggers]

Налаштування рівнів логування та формату лог-файлів

Документація див. [Python Logging Configuration](https://docs.python.org/2/library/logging.config.html)


```ini
; search.ini
[loggers]
keys = root, openprocurement.search

[handlers]
keys = stdout, stderr

[formatters]
keys = generic

[logger_root]
level = WARNING
handlers = stdout, stderr

[logger_openprocurement.search]
level = INFO
qualname = openprocurement.search
handlers =

[handler_stdout]
class = StreamHandler
args = (sys.stdout,)
level = NOTSET
formatter = generic

[handler_stderr]
class = StreamHandler
args = (sys.stderr,)
level = WARNING
formatter = generic

[formatter_generic]
format = %(asctime)s %(levelname)s [%(processName)s %(process)d] %(message)s
```


<a name="history"></a>

## Історія версій

* 05.02.2017 - додано index_lang
* 11.01.2017 - додано decode_orgs
* 20.12.2016 - додано user_agent
* 07.12.2016 - додано fast_mode client
* 21.11.2016 - додані утиліти test_index, test_search
* 19.11.2016 - додано індекс auction (prozorro.sale)
* 10.11.2016 - додано async_reindex
* 12.09.2016 - додана утиліта update_orgs
* 14.06.2016 - додано master/slave mode
* 22.03.2016 - додано утиліту ocds_ftp_sync та файл ftpsync.ini
* 22.12.2015 - перша версія пошукового сервісу


---
&copy; 2015-2017 Volodymyr Flonts / <flyonts@gmail.com> / https://github.com/imaginal
