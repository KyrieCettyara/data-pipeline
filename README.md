## Description
Startup ecosystem dikenal dengan pertumbuhan yang pesat dan  aktivitas yang beragam. Dalam ekosistem ini, sejumlah besar data dihasilkan, mencerminkan aktivitas dari perusahaan, investasi yang mereka terima, dan tokoh-tokoh utama yang terkait dengan mereka. Data ini sangat penting untuk memahami dinamika pasar, mengidentifikasi tren, dan membuat keputusan yang tepat

## Data Source 
1. Startup Investment Database: Berisi informasi rinci tentang perusahaan dan proses investasi yang mereka lalui. Ini mencakup data tentang  pendanaan, jumlah investasi, profil investor, dan Initial Public Offering (IPO).
2. File excel: Informasi tentang individu yang terkait dengan perusahaan-perusahaan ini, seperti pendiri, CEO, dan board members, disimpan dalam file excel terpisah. Data ini sangat penting untuk memahami elemen manusia di balik perusahaan, termasuk kepemimpinan dan pengaruh jaringan.

## Profiling
Profiling dilakukan untuk mendapatkan informasi mengenai data source. Sorce data akan melihat tipe data dari setiap atribute, unique value, persentase missing value, mem-validasi date format, dan terakhir membuat report hasil profiling.

Berikut merupakan salah satu contoh code.
~~~
##profiling database
# Extract list of table in database
list_table = extract_list_table(db_name=SRC_POSTGRES_DB)

# Profiling Table company
df_company = extract_database(SRC_POSTGRES_DB, 'company')

# create profiling object
complany_profiling = Profiling(data = df_company, table_name='company')

# get columns from the table
complany_profiling.get_columns()

# Set Profiling Rule
# list check data type (all columns)
data_type_column = complany_profiling.get_columns()

#list check unique values
unique_values = []

#list check percentage missing values
missing_values = ['country_code', 'state_code', 'region']

#list check valid date values
valid_date = ['created_at','updated_at']

# Set Profiling rule to object
complany_profiling.selected_columns(data_type_column, unique_values, missing_values, valid_date)

# Create Reporting Profiling
report_company = complany_profiling.reporting()
~~~

Hasil dari profiling kemudian disimpan kedalam MinIO

![alt text](https://github.com/KyrieCettyara/data-pipeline/blob/main/image/image1.png)

Sample profiling

![alt text](https://github.com/KyrieCettyara/data-pipeline/blob/main/image/profiling_result.png)

## Design Pipeline
Pipeline akan melakukan extract data dari data source database dan file excel. Raw data yang telah berhasil diextract kemudian di load kedalam satu database staging. Data yang ada dalam database staging kemudian diextract kemudian dilakukan transform dan kemudian diload kedalam database warehouse. Dari setiap proses extract, load, dan transform akan dilakukan logging yang akan disimpan didalam database log.

![alt text](https://github.com/KyrieCettyara/data-pipeline/blob/main/image/Startup_Investment_DB.png)



