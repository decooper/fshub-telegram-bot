# gunicorn.conf.py — конфиг для Render Start Command
#
# Render Start Command:
#     gunicorn app:app -c gunicorn.conf.py
#
# Заменяет прежний набор флагов
# (--workers 1 --threads 4 --timeout 120 --preload).

workers     = 1
threads     = 4
timeout     = 120
preload_app = True


def post_fork(server, worker):
    """
    Каждый воркер создаёт СВОЙ пул соединений после fork().

    Под preload_app модуль импортируется в мастер-процессе, где уже создан
    пул (нужен для _init_db на старте и для планировщика, живущего в мастере).
    Воркер при форке наследует объект пула с сокетами, открытыми ДО fork(),
    которые нельзя делить между процессами. Пересоздаём пул в воркере, чтобы
    у него были собственные соединения.
    """
    import core
    core._create_pool()
