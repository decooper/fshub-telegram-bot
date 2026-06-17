# gunicorn.conf.py — конфиг для Render Start Command
workers       = 1
threads       = 4
timeout       = 120
preload_app   = True

def post_fork(server, worker):
    """
    Каждый воркер создаёт СВОЙ пул соединений после форка.
    Иначе воркер наследует пул мастера с сокетами, открытыми до fork(),
    которые нельзя делить между процессами.
    """
    import core
    core._create_pool()
