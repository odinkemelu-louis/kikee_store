import pandas as pd
import psycopg2
from credentials import default

# db_connection = {'dbname':credentials['dbname'], 'password':credentials['password'], 'port':credentials['port'], 'user':credentials['user'],'host':credentials['host']}
# db_connect = credentials('dbname', 'password','user', 'port', 'host')

def extract(file_path):
    df = pd.read_csv(file_path)
    null = df.isnull().sum()
    df.fillna({
         'ProductID':0.0,
    'ProductName':'Unknown',
    'Category':'Unknown',
    'SubCategory':'Unknown',
    'Brand':'Unknown',
    'Price':0.0,
    'Discount':0.0,
    'Stock':0.0,
    'Rating':0.0,
    'ReviewCount':0.0,
    'UserID':0.0,
    'UserName':'Unknown',
    'UserAge':0.0,
    'serGender':'Unknown',
    'UserLocation':'Unknown',
    'CartID':0.0,
    'CartDate':'Unknown',
    'CartTotal':0.0,
    'CartItemCount':0.0,
    'CartStatus':'Unknown',
    'OrderID':0.0,
    'OrderDate':'Unknown',
    'OrderStatus':'Unknown',
    'PaymentMethod':'Unknown',
    'ShipmentDate':'Unknown',
    'DeliveryDate':'Unknown',
    'ReturnDate':'Unknown',
    'RefundAmount':0.0,
    'ReferralSource':'Unknown',
    'PromotionCode':'Unknown',
    'UserGender':'Unknown'
    },inplace=True)

    #Normalization
    ## sales table
    sales_dim = df[['CartDate','CartTotal','CartItemCount','CartStatus']].copy().drop_duplicates().reset_index(drop=True)
    sales_dim['Cart_ID'] = [i for i in range(1, len(sales_dim)+1)]
    sales_dim = sales_dim[['Cart_ID','CartDate','CartTotal','CartItemCount','CartStatus']]
    
    #product table
    product_dim = df[['ProductID', 'ProductName', 'Category', 'SubCategory', 'Brand', 'Price','Discount', 'Stock', 'Rating', 'ReviewCount']].copy().drop_duplicates().reset_index(drop=True)

    #order table
    order_dim = df[['OrderID', 'OrderDate','OrderStatus', 'PaymentMethod', 'ShipmentDate', 'DeliveryDate','ReturnDate', 'RefundAmount', 'ReferralSource', 'PromotionCode']].copy().drop_duplicates().reset_index(drop=True)
    
    #customer table
    customer_dim = df[['UserID', 'UserName','UserAge', 'UserGender', 'UserLocation']].copy().drop_duplicates().reset_index(drop=True)


    #saving the dataframe to csv format
    product_table = product_dim.to_csv('data/product_table.csv', index=False)
    sales_table = sales_dim.to_csv('data/sales_table.csv', index=False)
    customer_table = customer_dim.to_csv('data/customer_table.csv',index=False)
    order_table = order_dim.to_csv('data/order_table.csv',index=False)


##database connection

def load(dbname, port,user,host,password):
    conn = psycopg2.connect(dbname=dbname,port=port,user=user,host=host,password=password)
    return conn

##database connection and creation
def connection(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            drop table if exists product_table;
            drop table if exists order_table;
            drop table if exists customer_table;
            drop table if exists sales_table;
            """
        )
        cur.execute(
            """
        create table product_table(
        ProductID float,
        ProductName varchar(100),
        Category varchar(30),
        SubCategory varchar(20),
        Brand varchar(100),
        Price float,
        Discount float,
        Stock float,
        Rating float,
        ReviewCount float
        );

        create table customer_table(
        UserID float,
        UserName varchar(30),
        UserAge float,
        UserGender varchar(10),
        UserLocation varchar(20)
        );

        create table order_table(
        OrderID float,
        OrderDate varchar(20),
        OrderStatus varchar(20),
        PaymentMethod varchar(20),
        ShipmentDate varchar(20),
        DeliveryDate varchar(20),
        ReturnDate varchar(20),
        RefundAmount float,
        ReferralSource varchar(20),
        PromotionCode varchar(20)
        );

        create table sale_table(
        Cart_ID int,
        CartDate varchar(20),
        CartTotal float,
        CartItemCount float,
        CartStatus varchar(20)
        );
        """
        )

    conn.commit()
    # cur.close()
    # conn.close()




def load_csv(file_path, table_name, conn):
    with conn.cursor() as cur:
        with open(file_path, 'r') as f:
            next(f)
            cur.copy_expert(
                f"copy {table_name} FROM STDIN WITH CSV",
                f
            )

        conn.commit()
    print(f"data successfully loaded to {table_name}")


    
def main():
    #file extraction and cleaning
    file_path = 'kike_stores_dataset.csv'
    etl = extract(file_path)
    ## creating database connection and table creation

    conn = load(
        dbname=default['dbname'],
        user=default['user'],
        port=default['port'],
        password=default['password'],
        host=default['host']
    )
    ##database creating
    connection(conn)

    ##populating database with data
    load_csv('data/customer_table.csv','customer_table',conn)
    load_csv('data/product_table.csv','product_table',conn)
    load_csv('data/order_table.csv','order_table',conn)
    load_csv('data/sales_table.csv','sale_table',conn)

    
if __name__=="__main__":
    main()

