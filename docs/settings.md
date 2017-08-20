# Налаштування


Розміщення

* При інсталяції через buildout
    в `./openprocurement.search.buildout/etc`
* При інсталяціїї debian пакета
    в `/etc/search-tenders`


Перелік файлів налаштувань

* `accesslog.conf` - налаштування формату логу пошукових запитів
* `circus.ini` -  налаштування менеджера процесів
* `ftpsync.ini` - налаштування FTP синхронізації OCDS файлів
* `logrotate.conf` - правила ротації логів
* `search.ini` - основний файл налаштувань
* `/etc/cron.d/search-tenders` - періодичні операції


## accesslog.conf

Встановлює розміщення і формат лог файлу пошукових запитів, додає до стандартного формату час виконання кожного запиту

```ini
accesslog = '/var/log/search_access.log'
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(L)s'
```

Документація по [gunicorn settings](http://docs.gunicorn.org/en/stable/settings.html#access-log-format)

Підключається в `circus.ini`


## circus.ini

Circus запускає, моніторить роботу і перезапускає в разі аварійного завершення два основні сервіси:

1. Пошукове HTTP API, що запускається за допомогою gunicorn, приймає і обробляє пошукові запити, також видає статистику по `GET /heartbeat`
2. Індексуючий демон, що підтримує наповненя індексу в ElasticSearch, веде його перевірку і переіндексацію за розкладом

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

Типові налаштування рідко змінюються, є можливість додавати власні секції і таким чином


## ftpsync.ini

Використовувався до липня 2016, дозволяв отримувати вигрузку паперових продцедур з https://ips.vdz.ua/

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

## logrotate.conf

Стандартний [logrotate синтаксис](https://linux.die.net/man/5/logrotate.conf)

Винесений окремо для можливості ротації лог-файлів в нестандартний час (рекомендується в 23:50)

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


## search.ini

Основний файл налаштувань, містить в одному місці спільні налаштування для сервісів:

1. Пошукового HTTP API
2. Індексатора
3.  Додаткових утиліт
3.1. видалення старих індексів `clean_indexes`
3.2. перевірки наповнення індексів `test_index`
3.3. перевірки наявності останніх документів `test_search`
3.4. перервірки швидкості роботи `test_load`
3.5. перевірки назв організацій `update_orgs`


### [server:main]

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


### [app:main]

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

**name** - назва сервісу що потім показується у відповіді `heartbeat`

**secret_key** - ключ доступу до розширеної статистики `heartbeat` передається параметром `key` в `GET /heartbeat?key=123...`

**debug** - включення цього параметру додає відладочну інформацію у відповілях Search API


### [search_engine]

Основний розділ пошукового сервісу та індексатора, містить налаштування джерел даних, періодичності переіндексування, режиму Master-Slave, тощо.


#### Master-Slave

Два індексатора можуть працювати в режимі *master/slave*. В такому режимі *master* індексує, а *slave* моніторить його роботу і чекає. Якщо за якихось причин *master* перестає працювати тоді *slave* прокидається і продовжує індексування.  Коли *master* повертається до роботи, *slave* засипає.

```ini
[search_engine]
slave_mode = http://10.20.30.40:8484/heartbeat?key=123...
slave_wakeup = 600
```

**slave_mode** - URL для моніторингу *master* ноди. Прописується тільки для slave ноди, на master має бути закоментовано.

**slave_wakeup** - час простою *master* (секунд) при якому *slave* прокидається, рекомендується не менше 300 сек.


#### ElasticSearch


```ini
index_names  = /opt/search-tenders/var/index_names

elastic_host = 127.0.0.1:9200
;index_elastic_host = 127.0.0.1:9200
;search_elastic_host = 127.0.0.1:9200
elastic_timeout = 300
```

**index_names** - шлях до файлів які визначають поточний стан індексатора, цей префікс спільний для файлів

 * `index_names.yaml` - список імен індексів (поточних, попередніх, наступних, переіндексація яких ще йде)
 * `index_names.heartbeat` - час останньої успішної операції індексування
 * `index_names.lock` - pid-файл що захищає від повторного запуску індексатора

**elastic_host** - підключення до кластеру ElasticSearch

**elastic_timeout** - таймаут операцій ElasticSearch


#### EDRPOU database

```ini
orgs_db = /opt/search-tenders/edrpou/edrpou.db
orgs_queue = 1000
```

**orgs_db** - шлях до SQLite бази даних з офіційними назвами юридичних осіб для розшифровки імен організацій за ЄДРПОУ

**orgs_queue** - розмір черги індексатора підсказок назв організацій


#### Індекс "паперові тендери"

Шлях та налаштування джерела OCDS файлів паперових продцедур, які були отримані за допомогою утиліти `ocds_ftp_sync` див. файл налаштувань `ftpsync.ini`

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


#### Індекс "Тендери ProZorro"

```ini
;; tender source
;; =============
tender_api_key = ""
tender_api_url = https://public.api.openprocurement.org
tender_api_version = 2.3
;tender_api_mode = _all_
;tender_skip_until = 2016-01-01
;tender_user_agent =
;tender_fast_client = 1
;tender_decode_orgs = 1
;tender_file_cache = /mnt/cache/tenders
;tender_cache_allow = complete,cancelled,unsuccessful
;tender_cache_minage = 15
;tender_index_lang = english,russian,ukrainian
;tender_preload = 10000
;tender_limit = 1000
;tender_reindex = 5,6
;tender_check = 300000,2
;tender_resethour = 22
```


#### Індекс "Плани ProZorro"

```ini
;; plan source
;; ===========
plan_api_key = ""
plan_api_url = https://public.api.openprocurement.org
plan_api_version = 2.3
;plan_api_mode = _all_
;plan_skip_until = 2016-01-01
;plan_user_agent =
;plan_fast_client = 1
;plan_decode_orgs = 1
;plan_file_cache = /mnt/cache/plans
;plan_cache_minage = 15
;plan_index_lang = english,russian,ukrainian
;plan_preload = 10000
;plan_limit = 1000
;plan_reindex = 12,7
;plan_check = 500000,2
;plan_resethour = 23
```


#### Індекс "Аукціони ProZorro.Sale"

```ini
;; auction source
;; ==============
;auction_api_key = ""
;auction_api_url = http://public.api-sandbox.ea.openprocurement.org
;auction_api_version = 0
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
;auction_resethour = 22
;auction_check = 1,10
```


#### Індекс "Аукціони2 ProZorro.Sale (майно)"

```ini
auction2_api_url = http://public.api.ea2.openprocurement.org
```

Налаштування аналогічно індексу "Аукціони Prozorro.Sale"


#### Загальні налаштування індексатора

```ini
;; search.ini
;; common settings
;; ===============
;force_lower = 1
;async_reindex = 1
;ignore_errors = 1
;check_on_start = 1
;reindex_check = 1,1
;number_of_shards = 6
;index_parallel = 1
;index_speed = 500
;bulk_insert = 1
;update_wait = 5
;start_wait = 5
timeout = 30
```


### Розділ утиліти `update_orgs`

```ini
; search.ini
[update_orgs]
pidfile = /opt/search-tenders/var/run/update_orgs.pid
update_days = 30
```

**update_days** - за скільки днів перевіряти організації в тендерах


### Python logger settings (search.ini)

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

