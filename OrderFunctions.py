import pandas as pd
import random
import os
import logging
from datetime import datetime
from functions import ClientManager
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup

# Logging configuration
logger = logging.getLogger(__name__)

class OrderManagement:
    def __init__(self, csv_filename: str ,
                 client_manager: ClientManager):
        
        self.client_manager = client_manager
        self.csv_filename = csv_filename
        self.df = self.load_data()

    def load_data(self):
        """Load data from CSV or create a new DataFrame with columns if the file does not exist."""
        if os.path.exists(self.csv_filename):
            try:
                df = pd.read_csv(self.csv_filename)
                logger.info("CSV data loaded successfully.")
                return df
            except Exception as e:
                logger.error(f"Error reading CSV file: {str(e)}")
                return pd.DataFrame(columns=['Order_date','Account_ID', 'Order_Ticket' ,'Client_name',
                                             'Order_type', 'Order_currency', 'Order_size',
                                             'Order_price', 'Payable_to_Toman', 'Status','paid_by_client', 'Dept'])
        else:
            logger.info("CSV file not found. Creating a new one.")
            return pd.DataFrame(columns=['Order_date','Account_ID', 'Order_Ticket' ,'Client_name', 
                                         'Order_type', 'Order_currency', 'Order_size', 
                                         'Order_price', 'Payable_to_Toman', 'Status','paid_by_client', 'Dept'])

    def save_data(self):
        """Save the DataFrame to CSV."""
        try:
            self.df.to_csv(self.csv_filename, index=False)
            
            logger.info(f"Data saved successfully to {self.csv_filename}.")
        except Exception as e:
            logger.error(f"Error saving CSV file: {str(e)}")
            return False
        return True

    def create_order(self, client_id: int, order_type: str, order_currency: str, order_size: float, order_price: float, status: str, paid_by_client: float) -> int:
        """Create a new order and save it to the CSV."""
        account_id = self.client_manager.get_account_id_by_client_id(client_id)
        order_ticket = random.randint(100000, 999999)  # Generate random order ticket
        order_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        client_name= self.client_manager.get_client_name(client_id)
        # Calculate payable

        payable_to_toman = order_size * order_price 
        dept = payable_to_toman - paid_by_client

        
        new_order = pd.DataFrame([[order_date, account_id, order_ticket , client_name, order_type,
                                    order_currency, order_size, order_price, payable_to_toman, status, paid_by_client, dept]],
                                    columns=['Order_date','Account_ID', 'Order_Ticket' ,'Client_name',
                                            'Order_type', 'Order_currency', 'Order_size',
                                            'Order_price', 'Payable_to_Toman', 'Status', 'paid_by_client', 'Dept'])
        
        
        # Append the new order to the existing DataFrame
        self.df = pd.concat([self.df, new_order], ignore_index=True)
        logger.info(f"Order created successfully with ticket: {order_ticket}.")

        self.save_data()
        return order_ticket  # Return generated order ticket

    def format_order_details(self, order_ticket: int):
        """Format the details of an order given its ticket."""
        order_info = self.df[self.df['Order_Ticket'] == order_ticket]
        if not order_info.empty:
            return order_info.iloc[0].to_string(index=False)
        return 'Order details not found.'
    
    def parse_order_input(self, order_input: str):
        # Extract the four parameters from the input string
        parts = order_input.split()

        if len(parts) != 4:
            raise ValueError('Invalid order format. Please use: "ORDER_TYPE ORDER_SIZE ORDER_CURRENCY ORDER_EXCHANGE_RATE".')

        order_type = parts[0].upper()

        if order_type not in ['B', 'S', 'BUY', 'SELL']:
            raise ValueError('Order type must be BUY|B or SELL|S.')
        
        if order_type == 'B' : order_type = 'BUY'
        if order_type == 'S' : order_type = 'SELL'

        try:
            order_size = float(parts[1])
            order_price = float(parts[3])
        except ValueError:
            raise ValueError('Order size and price must be numeric.')
        
        
        order_currency = parts[2].lower()  
        order_currency = order_currency.upper()

        return order_type, order_size, order_currency, order_price
    
    async def handle_order_edit(self, query, client_id: int) -> None:
        account_id = self.client_manager.get_account_id_by_client_id(client_id)
        order_history = self.get_latest_orders(account_id, 0)

        # Debugging logs to inspect columns
        logger.debug(f"Order history columns: {order_history.columns.tolist()}")
        
        if order_history.empty:
            await query.edit_message_text("No orders available to edit.")
            return
        
        keyboard = []
        for index, row in order_history.iterrows():
            try:
                keyboard.append([InlineKeyboardButton(f"{(row['Order_price'])} --|-- Size: {int(row['Order_size']):,}",
                    callback_data=f'remove_order_{int(row["Order_Ticket"])}_{client_id}')])
            except KeyError as e:
                logger.error(f"KeyError accessing row in order history: {e}")
                await query.edit_message_text(f"Error: The column 'Order_Ticket' was not found in the order history.")
                return
        keyboard.append([InlineKeyboardButton("Back", callback_data=f'order_history_{client_id}')])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("Select an order to delete:", reply_markup=reply_markup)

    
    
    def delete_order(self, order_ticket: int):
        order_row = self.df[self.df['Order_Ticket'] == order_ticket]

        if order_row.empty:
            return None  # Handle case where the order_ticket does not exist

        order_status = order_row['Status'].values[0]
        Payable_to_Toman = order_row['Payable_to_Toman'].values[0]
        paid_by_client = order_row['paid_by_client'].values[0]
        order_type = order_row['Order_type'].values[0]

        payable_amount = Payable_to_Toman - paid_by_client   # paid by client is 0 in pending

        # Remove the row permanently
        self.df = self.df.drop(self.df[self.df["Order_Ticket"] == order_ticket].index)

        self.save_data()

        return order_status, order_type, payable_amount

    


    
    def get_HexAccount_info(self, client_id):
        if os.path.exists('Hex_account.csv'):
                Accounts = pd.read_csv('Hex_account.csv')
                logger.info("HEXAccounts data loaded successfully.")
                client_row = Accounts[Accounts['Client_id'] == client_id]
                if not client_row.empty:
                   account_id = client_row['account_id'].values[0] 
                   account_info = Accounts[Accounts['account_id'] == account_id]
                    
                   return account_id , account_info.iloc[0]
            
        else:
            logger.info("HEX CSV file not found.")
            return None

    async def get_order_history(self, client_id: int , offset: int):
        account_id = self.client_manager.get_account_id_by_client_id(client_id)
        client_info = self.client_manager.get_client_details(client_id) 
        client_name = f"Client Name:  {client_info['Client_name']}\n"
        

        total_sell_usdt, total_buy_usdt = self.get_ordertotals_for_today(account_id)
        
        latest_orders = self.get_latest_orders(account_id,offset)

        if client_info is None:
            return None, None

        summary = (
             client_name +
            f"Account ID:     {account_id}\n"
            f"-------------------------------------------------------------------------\n"
            f"Balance (USDT):                       {int(client_info['USDT_Balance']):,}\n"
            f"Balance (TOMAN):                   {int(client_info['Toman_Balance']):,}\n\n"
            f"-------------------------------------------------------------------------\n"
            f"                        Total (Today)\n"  
            f"-------------------------------------------------------------------------\n"    
            f"USDT (BUY):                          {int(total_buy_usdt):,}\n"  
            f"USDT (SELL):                          {int(total_sell_usdt):,}\n"
        )

        return summary, latest_orders
    
    def get_latest_orders(self, account_id: int, offset: int):
        orders = self.df[self.df['Account_ID'] == account_id][[
        'Order_date', 'Order_Ticket', 'Order_type', 'Order_currency', 'Order_size', 'Order_price'
    ]]
        sorted_orders = orders.sort_values(by='Order_date', ascending=False)
        latest_orders = sorted_orders.iloc[offset:offset + 3]
      
        return latest_orders
    
    def get_ordertotals_for_today(self, account_id: int):

        today = datetime.now().date().strftime("%Y-%m-%d")
        order_today_USDT = self.df[self.df['Order_date'].str.startswith(today) & 
                                    (self.df['Account_ID'] == account_id) &
                                    (self.df['Order_currency'] == 'USDT')]
        

        total_sell_usdt =  order_today_USDT[order_today_USDT['Order_type'] == 'SELL']['Order_size'].sum()
        total_buy_usdt =  order_today_USDT[order_today_USDT['Order_type'] == 'BUY']['Order_size'].sum()
        

        return total_sell_usdt, total_buy_usdt
    









