import pandas as pd
import random
from datetime import datetime,  timedelta
import os
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from OrderFunctions import OrderManagement
from functions import ClientManager


# Logging configuration
logger = logging.getLogger(__name__)

class HexAccountManager:
    def __init__(self,
                 csv_filename: str,
                 order_management: OrderManagement,
                 client_manager:ClientManager):
        
        self.csv_filename = csv_filename
        self.order_management = order_management
        self.client_manager = client_manager
        self.df = self.load_data()
        self.input_state = {}


    def load_data(self):
        try:
            if os.path.exists(self.csv_filename):
                return pd.read_csv(self.csv_filename)
            else:
                return pd.DataFrame(columns=[
                    'DATE', 'Bought_USDT', 'Sold_USDT', 'Net_position'
                    ])
        except Exception as e:
            logger.error(f"Error reading Hex account CSV file: {str(e)}")
            return pd.DataFrame(columns=[
                'DATE', 'Bought_USDT', 'Sold_USDT', 'Net_position'
            ])

    def save_data(self):
        try:
            self.df.to_csv(self.csv_filename, index=False)
        except Exception as e:
            logger.error(f"Error saving Hex account CSV file: {str(e)}")
            return False
        return True


    def add_net_position(self,
                       date: str,  # datetime.now().date().strftime("%Y-%m-%d")
                       update_bought: float, 
                       update_sold: float, 
                       update_net: float ):
        
        # Create a boolean mask to check if the date exists
        date_mask = self.df['DATE'] == date
        if date_mask.any():  # Check if any rows match the date
        # Use the boolean mask for updating
            self.df.loc[date_mask, 'Bought_USDT'] = update_bought
            self.df.loc[date_mask, 'Sold_USDT'] = update_sold
            self.df.loc[date_mask, 'Net_position'] = update_net
            self.save_data()
            return None

         
        
        new_data = pd.DataFrame([[date, update_bought, update_sold, update_net ]],
                                   columns=['DATE', 'Bought_USDT', 'Sold_USDT', 'Net_position'])

        self.df = pd.concat([self.df, new_data], ignore_index=True)
        self.save_data()
        return date  # Return new date

     
    def load_transactions(self):
        from Hex import generate_csv
        transaction_csv = generate_csv('Transactions')
        if os.path.exists(transaction_csv ):
            try:
                transactions = pd.read_csv(transaction_csv)
                logger.info("Transaction data loaded successfully.")
                return transactions
            except Exception as e:
                logger.error(f"Error reading Transactions CSV file: {str(e)}")
                return pd.DataFrame(columns=['transaction_date','Account ID', 'Order Ticket', 'Client_name', 'transaction_type',
                                             'transaction_currency', 'transaction_size'])
        else:
            logger.info("Transactions CSV file not found. Creating a new one.")
            return pd.DataFrame(columns=['transaction_date','Account ID', 'Order Ticket', 'Client_name', 'transaction_type',
                                             'transaction_currency', 'transaction_size'])
   
   
    def get_totals_orders_for_all(self):
        orders = self.order_management.load_data()
        all_sold_usdt =  orders[
                               (orders['Order_type'] == 'SELL') &
                               (orders['Order_currency'] == 'USDT')
                                ]['Order_size'].sum()
        
        all_bought_usdt = orders[(orders['Order_type'] == 'BUY') & 
                                 (orders['Order_currency'] == 'USDT')
                             ]['Order_size'].sum()
        
        total_orders = [all_sold_usdt, all_bought_usdt]
        
        return total_orders
        
    def get_totals_for_today(self):
        
        today = datetime.now().date().strftime("%Y-%m-%d")
        transactions = self.load_transactions()
        orders = self.order_management.load_data()


        ## ----- Total transaction for Today -------

        transactions_today_usdt = transactions[transactions['transaction_date'].str.startswith(today) & 
                                    (transactions['transaction_currency'] == 'USDT')]
        
        transactions_today_toman = transactions[transactions['transaction_date'].str.startswith(today) & 
                                    (transactions['transaction_currency'] == 'TOMAN')]
        
        
       ## ------ total Orders for today --------

        order_today_USDT = orders[orders['Order_date'].str.startswith(today) & 
                                    (orders['Order_currency'] == 'USDT')]
        
        
        ## ----- total SOLD / BOUGHT Today ------

        total_sell_usdt =  order_today_USDT[order_today_USDT['Order_type'] == 'SELL']['Order_size'].sum()
        total_buy_usdt =  order_today_USDT[order_today_USDT['Order_type'] == 'BUY']['Order_size'].sum()
        
        ## ---- Total withdraw/Deposit Today --------

        total_usdt_deposit = transactions_today_usdt[transactions_today_usdt['transaction_type'] == 'Send']['transaction_size'].sum()
        total_usdt_withdraw = transactions_today_usdt[transactions_today_usdt['transaction_type'] == 'Receive']['transaction_size'].sum()

        total_toman_deposit = transactions_today_toman[transactions_today_toman['transaction_type'] == 'Send']['transaction_size'].sum()
        total_toman_withdraw = transactions_today_toman[transactions_today_toman['transaction_type'] == 'Receive']['transaction_size'].sum()



        total_transactions = [total_toman_deposit , total_usdt_deposit,total_toman_withdraw, total_usdt_withdraw ]
        total_orders = [total_sell_usdt,total_buy_usdt]
      
        return   total_transactions , total_orders
    
    
    def Hex_summary(self ) :
        total_transactions , total_today_orders = self.get_totals_for_today()
        total_orders = self.get_totals_orders_for_all()
 
        client_Data = self.client_manager.load_data()

        # Gets the total balance for all the clients to get payables and depts

        balance_usdt = client_Data['USDT_Balance'].sum()
        balance_toman = client_Data['Toman_Balance'].sum()
        
        # total sold and bought for just TODAY

        today_sold_usdt = total_today_orders[0]
        today_bought_usdt = total_today_orders[1]


        # All sold and bought orders for all

        total_sold_usdt = total_orders[0]
        total_bought_usdt = total_orders[1]
       
        # Total Transactions for Today


        total_toman_deposit = total_transactions[0]
        total_usdt_deposit = total_transactions[1]
        total_toman_withdraw= total_transactions[2]
        total_usdt_withdraw = total_transactions[3] 

       
        today = datetime.now().date().strftime("%Y-%m-%d")
        yesterday = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")


        Today_net_position =  today_bought_usdt - today_sold_usdt 

        Yesterday_net_position =  self.get_previous_net_position(yesterday)
        print("Latest Net Position: ", Yesterday_net_position)

        Today_net_position = Today_net_position + Yesterday_net_position

        # Update Net Position
        
        date = self.add_net_position(
                              today, 
                              today_bought_usdt, 
                              today_sold_usdt,
                              Today_net_position)

        
        
        summary = (
            f"------------------------- Client Balances ------------------------\n\n"
            f"Balance (TOMAN):                    {int(balance_toman):,}\n\n"
            f"-------------------------------------------------------------------------\n"
            f"                          TODAY SUMMARY (USDT) \n"                  
            f"-------------------------------------------------------------------------\n" 
            f"Net Position:                            {int(Today_net_position):,}\n"   
            f"SOLD:                                         {int(today_sold_usdt):,}\n"
            f"BOUGHT:                                   {int(today_bought_usdt):,} \n"      
             f"-------------------------------------------------------------------------\n"
            f"                        TOTAL TRANSACTIONS TODAY \n"                  
            f"-------------------------------------------------------------------------\n"   
            f"TOMAN (Sent):                 {int(total_toman_deposit):,}\n"
            f"TOMAN (Received):         {int(total_toman_withdraw):,} \n"
            f"USDT (Sent):                     {int(total_usdt_deposit):,}\n"
            f"USDT (Received):             {int(total_usdt_withdraw):,} \n"      
            
            
        )
        return summary, Today_net_position


    async def Show_Hex_data(self, query) :

        keyboard = [
            [InlineKeyboardButton("Payables", callback_data='show_payables'),
            InlineKeyboardButton("Receivables", callback_data='show_receivables')],
            [InlineKeyboardButton("CSV Report", callback_data='generate_csv_report')],
            [InlineKeyboardButton("Back to Main Menu", callback_data='back_to_main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        summary_message = self.Hex_summary()[0]

        await query.edit_message_text(summary_message, reply_markup=reply_markup)


    def get_previous_net_position(self, yesterday):
        currentDate = pd.Timestamp(datetime.now().date())  # Convert to Timestamp
        first_of_current_month = currentDate.replace(day=1)

        last_month_str = (currentDate - timedelta(days=1)).strftime("%Y_%m")
        last_month_csv = 'Hex_dashboard' + '-' + last_month_str + '.csv'

        # Check if today is the first day of the month
        if currentDate == first_of_current_month:
            # Retrieve latest net position from the last month's CSV
            try:
                if os.path.exists(last_month_csv):
                    Hex_dashboard_csv = pd.read_csv(last_month_csv)
                    latest_net_position = Hex_dashboard_csv['Net_position'].iloc[-1]  # Adjust the column name as needed
                    return latest_net_position
                else:
                    return 0  # If no data exists for last month
            except Exception as e:
                logger.error(f"Error reading last month's Hex account CSV file: {str(e)}")
                return 0
            
         # Convert DATE column to datetime if it's not already
        self.df['DATE'] = pd.to_datetime(self.df['DATE'])
            
        # Filter out today's entries
        previous_data = self.df[self.df['DATE'] < currentDate]

        # Check if there are any entries prior to today
        if not previous_data.empty:
            return previous_data['Net_position'].values[-1]
        else:
            return 0  # If there are no entries prior to today
        
    
    async def show_payables(self, query):
        client_data = self.client_manager.df
        payables = client_data[client_data['Toman_Balance'] > 0]

        keyboard = [ 
            [InlineKeyboardButton("Clients List", callback_data='list_clients'),
             InlineKeyboardButton("Back", callback_data='hex_account_summary')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        payables_list = "-------------------------------------------------------\n"
        payables_list +="   *BALANCE*             *NAME*   \n"
        payables_list +="-------------------------------------------------------\n"

        if payables.empty:
            await query.edit_message_text("No payables found.", reply_markup= reply_markup)
        else:
            payables_list += "\n".join([f"{int(row['Toman_Balance']):,}     |   {row['Client_name']}" for index, row in payables.iterrows()])
            await query.edit_message_text(f"Clients with Payables(TOMAN):\n\n{payables_list}", reply_markup= reply_markup)

    # New function to show clients with positive balances (Receivables)
    async def show_receivables(self, query):
        client_data = self.client_manager.df
        receivables = client_data[client_data['Toman_Balance'] < 0]
        
        keyboard = [ 
            [InlineKeyboardButton("Clients List", callback_data='list_clients'),
            InlineKeyboardButton("Back", callback_data='hex_account_summary')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        receivables_list = "-------------------------------------------------------\n"
        receivables_list +="   *BALANCE*             *NAME*   \n"
        receivables_list +="-------------------------------------------------------\n"


        if receivables.empty:
            await query.edit_message_text("No receivables found.", reply_markup= reply_markup)
        else:
            receivables_list += "\n".join([f"{int(row['Toman_Balance']):,}     |   {row['Client_name']}" for index, row in receivables.iterrows()])
            await query.edit_message_text(f"Clients with Receivables(TOMAN):\n\n{receivables_list}", reply_markup= reply_markup)
                
            
     
            
    
    
