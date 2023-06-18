class DevelopmentConfig():
    DEBUG = True
    server = 'challenge-data.database.windows.net'
    database = 'prueba-01'
    username = 'main'
    password = 'Loquita9277$'
    driver = '{ODBC Driver 17 for SQL Server}'

    connection_string = f"Driver={driver};Server={server};Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;"

config = {
    'development' : DevelopmentConfig
}