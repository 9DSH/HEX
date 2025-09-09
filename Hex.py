from typing import Final
import pandas as pd
import os
from datetime import datetime
import re
import logging
import time
import threading
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram import InlineQueryResultArticle, InputTextMessageContent
from telegram.ext import Application, CommandHandler, ContextTypes,InlineQueryHandler, CallbackQueryHandler, MessageHandler, filters
from functions import ClientManager, SearchManager
from OrderFunctions import OrderManagement
from PaymentFunctions import PaymentManager
from TransactionFunctions import TransactionManagement
from HexFunctions import HexAccountManager 
from SheetsManager import GoogleManager


# Constants
TOKEN: Final = 'Token'  # TELEGRAM TOKEN
# Create a lock instance

def generate_csv(csv_filename: str) -> str:
    currentDate = datetime.now().date().strftime("%Y_%m")
    return f"{csv_filename}-{currentDate}.csv"

CSV_FILENAME: Final = generate_csv('clients')
ORDER_FILENAME: Final = generate_csv('OrderHistory')
TRANSACTION_FILENAME: Final = generate_csv('Transactions')
HEX_ACCOUNT_FILENAME: Final = generate_csv('Hex_dashboard')
 

# Google Sheets Info   C:\Users\HAMED\Python\HEX 
current_dir = os.path.dirname(os.path.abspath(__file__))  # Get the current directory of the script
creds_path = os.path.join(current_dir, 'credentials.json') 
# sheet_id = '1T6VaQONirE1CYVL8MsGX4EXSOlO43uWOZ7zZAYb0h1Y'


# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Managers
google_manager = GoogleManager(creds_path)
client_manager = ClientManager(CSV_FILENAME)
search_manager = SearchManager(client_manager)
order_management = OrderManagement(ORDER_FILENAME, client_manager)
hex_account_manager = HexAccountManager(HEX_ACCOUNT_FILENAME,order_management,client_manager)
transaction_management = TransactionManagement(TRANSACTION_FILENAME,client_manager, hex_account_manager)
payment_manager = PaymentManager(order_management, client_manager, transaction_management, hex_account_manager) 

#google_manager.delete_all_spreadsheets()
#google_manager.create_monthly_spreadsheets()

# Function to run scheduled upload to Google Sheets
upload_lock = threading.Lock()
upload_thread = None 


# --------- Start Command --------- 
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Determine if update is from a callback query or a message
    net_position = hex_account_manager.Hex_summary()[1]
    message = (
                  f"Net Position:      {int(net_position):,}\n\n"
                  f"Please Choose an action:\n"
           )
    keyboard = [
        [InlineKeyboardButton("HEX Accounts", callback_data='hex_account_summary')],
        [InlineKeyboardButton("Clients List", callback_data='list_clients')],
        [InlineKeyboardButton("Transfer", callback_data='transfer'),
        InlineKeyboardButton("Search Clients", callback_data='search_clients')]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text(message, reply_markup=reply_markup)
    else:
        await update.message.reply_text(message, reply_markup=reply_markup)

###--------------- Search Command -----------------

async def search(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.args:  # Checking if there are any arguments provided with the command
        query = ' '.join(context.args)  # Joining arguments into a single string
        await search_manager.get_search_results(update, query)  # Actual search call
    else:
        await update.message.reply_text("Please provide a client name to search for.")
        user_input_state[update.message.from_user.id] = 'waiting_for_search_query' 


#------------------------------------------------
# ---------------- BUTTON CLICK ----------------- 
#------------------------------------------------

user_input_state = {}

async def handle_button_click(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()  # Acknowledge button click

    user_input_state.clear()  # Clear previous user input states

    if query.data == 'add_new_client':
        await query.edit_message_text('Please provide client\'s name in the format: "Name".')
        user_input_state[query.from_user.id] = 'waiting_for_name'

    elif query.data == 'back_to_main_menu':
        await menu(update, context) 
    elif query.data == 'list_clients': 
        await client_manager.list_clients(query)
    
    elif query.data.startswith('clients_page_'):
        offset = int(query.data.split('_')[2])  # Extract the offset
        await client_manager.list_clients(query, offset)
    elif query.data == 'search_clients':
        await query.edit_message_text('Please type the client name or ID to search for.')
        user_input_state[query.from_user.id] = 'waiting_for_search_query'


    #### ----------  Transfer ---------------
    elif query.data == 'direct_transfer':
        await query.edit_message_text("Please provide transfer details in the format: \"amount | currency | sender_name | receiver_name\" (e.g., \"500 USDT hamed amir\")")
        user_input_state[query.from_user.id] =  'transfer_money'

    elif query.data.startswith('confirm_transfer_'):
         parts = query.data.split('_')
         amount = float(parts[2])
         currency = parts[3]
         sender_id = int(parts[4] )
         receiver_id= int(parts[5])

        
          # Perform the transfer
         transaction_type_send = "Receive"
         transaction_type_receive = "Send"

         transaction_management.update_client_balance(sender_id, transaction_type_send, currency, amount)
         from_completion_text = await transaction_management.add_transaction(sender_id, 00000, transaction_type_send, currency, amount)

         transaction_management.update_client_balance(receiver_id, transaction_type_receive, currency, amount)
         to_completion_text = await transaction_management.add_transaction(receiver_id, 00000, transaction_type_receive, currency, amount)


         await query.edit_message_text(from_completion_text)
         await query.message.reply_text(to_completion_text)

    elif query.data.startswith('transfer_from_'):
        from_client_id = int(query.data.split('_')[2])
        await client_manager.handle_transfer(query, from_client_id)
    
    elif query.data.startswith('transfer_to_'):
        to_client_id = int(query.data.split('_')[2])
        from_client_id = int(query.data.split('_')[3])
        user_input_state[query.from_user.id] = ('waiting_for_transfer', from_client_id, to_client_id)

        await query.edit_message_text("Please enter the amount to transfer, e.g., '5000 USDT'.")


    #### ----------  Order ---------------

    elif query.data.startswith('order_history_'):
        parts = query.data.split('_')
        client_id = int(parts[2])
        await show_order_history(query, client_id,0)

    elif query.data.startswith('new_order_'):
        parts = query.data.split('_')
        client_id = int(parts[2])  # Extract the client_id from the callback data
        

        await query.edit_message_text('Please provide the order details in this format: "SELL 5000 USDT 66900".')
        user_input_state[query.from_user.id] = ('waiting_for_new_order', client_id)  # Save state
    
    elif query.data.startswith('complete_payment_'):
        await payment_manager.complete_payment(query,context)


    elif query.data.startswith('C_manual_'):
        parts = query.data.split('_')
        order_info = parts[2:] 
        payment_amount = float(parts[8])
        await payment_manager.process_payment_amount(query, payment_amount, order_info) 
 
    elif query.data.startswith('holding_payment_'):
        await payment_manager.process_holding_payment(query, context) 
        
            
    elif query.data.startswith('manual_'): 
        parts = query.data.split('_')
        
        order_info = parts[1:] 
        client_id = int(order_info[0])  # Using the correct index for client_id
        order_type = order_info[1]
        order_size = float(order_info[2])
        order_currency = order_info[3]
        order_price = float(order_info[4])
        payment_due = float(order_info[5])

        await query.edit_message_text(f'Please enter the payment amount in Toman (e.g. 10000000).')

        user_input_state[query.from_user.id] = ('waiting_for_payment', client_id, order_type,order_size,order_currency,order_price, payment_due)

    elif query.data == 'cancel_order':
        await payment_manager.handle_cancel_order(update, context)


    elif query.data.startswith('next_orders_'):
        parts = query.data.split('_')

        client_id = int(parts[2])  
        offset = int(parts[3])      
       
        await show_order_history(query, client_id, offset)  # Show the next set of Order
        

    elif query.data.startswith('previous_orders_'):
        parts = query.data.split('_')
        client_id = int(parts[2])
        offset = int(parts[3]) 

        await show_order_history(query, client_id, offset)  # Show the previous set

    elif query.data.startswith('edit_orders_'):
        parts = query.data.split('_')
        client_id = int(parts[2])
        await order_management.handle_order_edit(query, client_id)

    elif query.data.startswith('remove_order_'):
        parts = query.data.split('_')
        order_ticket = int(parts[2])
        client_id = int(parts[3])
        order_status, order_type, order_payable = order_management.delete_order(order_ticket)
         
        transaction_currency = 'TOMAN'

        if order_status == 'Pending' :
             # update client balance
            if order_type == 'BUY' : 
                transaction_type = "Receive"
                transaction_management.update_client_balance(client_id,transaction_type,transaction_currency,order_payable)
            elif order_type == 'SELL':
                transaction_type = "Send"
                transaction_management.update_client_balance(client_id,transaction_type,transaction_currency,order_payable)
        
        elif order_status == 'Manual':
            # update client balance
            if order_type == 'BUY' : 
                transaction_type = "Receive"
                transaction_management.update_client_balance(client_id,transaction_type,transaction_currency,order_payable)
            elif order_type == 'SELL':
                transaction_type = "Send"
                transaction_management.update_client_balance(client_id,transaction_type,transaction_currency,order_payable)
             # Remove Related Transaction
            transaction_management.remove_transaction(order_ticket)

        elif order_status == 'Complete':
             transaction_management.remove_transaction(order_ticket)

        update_net_position , summary = hex_account_manager.Hex_summary()
        await query.edit_message_text(f"Order {order_ticket} has been Deleted.")
        await menu(update, context)
            

    #### ----------- TRANSACTION ---------------

    # Handle New Transaction button
    elif query.data.startswith('new_transaction_'):
        client_id = int(query.data.split('_')[2])  # Get client ID from the callback data
      
        await query.edit_message_text('Please provide transaction details in the format: "send|s 5000 USDT".')
        user_input_state[query.from_user.id] = ('waiting_for_new_transaction', client_id)

    elif query.data.startswith('complete_transaction_'):
        parts = query.data.split('_')
        client_id = int(parts[2])
        transaction_type = parts[3]
        transaction_size = float(parts[4])
        transaction_currency = parts[5]
        order_ticket = 00000


        
        callback = 'list_clients'

        try:
            transaction_management.update_client_balance(client_id,transaction_type,transaction_currency,transaction_size)
            Completion_text = await transaction_management.add_transaction(client_id, order_ticket, transaction_type, transaction_currency, transaction_size)
            
            keyboard = [
                            [InlineKeyboardButton("New Transaction", callback_data=f'new_transaction_{client_id}'), 
                             InlineKeyboardButton("Account History", callback_data=f'account_history_{client_id}')], 
                            [InlineKeyboardButton("Back", callback_data=callback)]
                        ]
  
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(Completion_text, reply_markup=reply_markup)
        except ValueError as e:
            await query.edit_message_text(f'Error: {str(e)}')
        except Exception as e:
            await query.edit_message_text(f'An unexpected error occurred: {str(e)}')

    elif query.data.startswith('account_history_'):
        client_id = int(query.data.split('_')[2])  # Get client ID from the callback data
      
        await show_account_history(query, client_id, 0)
    
    elif query.data == 'cancel_transaction_':
        await transaction_management.handle_cancel_transaction(update, context)

    elif query.data.startswith('next_transactions_'):
        parts = query.data.split('_')
        

        client_id = int(parts[2])  # Extract client_id  
        offset = int(parts[3])      # Get the current offset for pagination
        
        await show_account_history(query, client_id, offset)  # Show the next set of transactions
        

    elif query.data.startswith('previous_transactions_'):
        parts = query.data.split('_')
       
        client_id = int(parts[2]) 
        offset = int(parts[3])      # Get the current offset for pagination

        await show_account_history(query, client_id, offset)  # Show the previous set
    ####---------- HEX ACCOUNTS ----------

    elif query.data == 'hex_account_summary':  # handler for Hex account summary
        await hex_account_manager.Show_Hex_data(query)
        
    elif query.data == 'show_payables':
        await hex_account_manager.show_payables(query)

    elif query.data == 'show_receivables':
        await hex_account_manager.show_receivables(query)

    elif query.data == 'generate_csv_report':
        await query.edit_message_text("Wait for Downloading Sheets and Creating Excel file....")
        await google_manager.handle_download_google_sheet(query, context)
   
    #### ---------- EDIT -----------------
    elif query.data.startswith('edit_'):
        parts = query.data.split('_')
        if len(parts) == 3:
            action = parts[1]  # edit name 
            client_id = int(parts[2])  # client ID
            
            if action == 'name':
                await client_manager.edit_client_name(query, client_id, user_input_state)
        else:
            await client_manager.present_edit_options(query, int(parts[1]))  # Pass the client ID for edit options
    else:
        try:
            client_id = int(query.data)  # Convert the callback data back to an integer
            await show_client_details(query, client_id)  # Show client details
        except ValueError:
            keyboard = [
                  InlineKeyboardButton("Back to Main Menu", callback_data='back_to_main_menu')
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text('Invalid action. Please try again.' , reply_markup=reply_markup)
    

#-------------------------------------------------------------------------
# ------------------------- Handle Messages -----------------------------  
#-------------------------------------------------------------------------

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    if user_id in user_input_state:
        state = user_input_state[user_id]
        if state == 'waiting_for_name': 
            client_info = update.message.text
            
            client_name = client_info
            balance = 0.0  # Set default balance here, you can ask user for balance input too
            client_id = client_manager.add_client(client_name, balance)
            keyboard = [
                 [InlineKeyboardButton("See Client list", callback_data='list_clients'),
                  InlineKeyboardButton("Back to Main Menu", callback_data='back_to_main_menu')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            if client_id is None:
                await update.message.reply_text('Client already exists.')
            else:
                await update.message.reply_text(text = f'Client {client_name} added with Client ID: {client_id}.',
                                                reply_markup=reply_markup
                                                )
                
            del user_input_state[user_id]  
        elif state[0] == 'waiting_for_edit_name': 
            new_name = update.message.text.strip()
            response = await client_manager.handle_edit_name(state[1], new_name)
            await update.message.reply_text(response) 
            del user_input_state[user_id]
        elif state == 'waiting_for_search_query': 
            await search_client(update, update.message.text.strip().lower())
            del user_input_state[user_id]

        elif state[0] == 'waiting_for_new_order':
            order_input = update.message.text.strip()
            client_id = state[1]  # Retrieve the client ID from the state
            await payment_manager.confirm_new_order(update, client_id, order_input)
            
            del user_input_state[user_id]  # Clear the state

        elif state[0] == 'waiting_for_payment':
            payment_amount = float(update.message.text.strip())
            order_info = state[1:]
            await payment_manager.confirm_manual_payment(update, payment_amount, order_info)
            del user_input_state[user_id]  # Clear the state

        elif state[0] == 'waiting_for_new_transaction':
             transaction_input = update.message.text.strip()
             client_id = state[1]
             try:
                # Add the transaction using the TransactionManagement class
                await transaction_management.confirm_new_transaction(update, client_id, transaction_input)
             except ValueError as e:
                await update.message.reply_text(str(e))  # Provide user feedback on the error
            
             del user_input_state[user_id]
        
        # ----------------- Transfer ---------------
        elif state ==  'transfer_money':
            transfer_info = update.message.text
            parts = transfer_info.split()
            if len(parts) != 4:
                raise ValueError("Message must be in the format: 'amount currency sender_name receiver_name'.")
            amount = float(parts[0])
            transfer_currency = parts[1]
            sender_name = parts[2]
            receiver_name = parts[3]

            sender_id = client_manager.get_client_id_by_name(sender_name)
            receiver_id = client_manager.get_client_id_by_name(receiver_name)

            await client_manager.confirm_transfer_message(update,amount, transfer_currency, sender_id, receiver_id)
            del user_input_state[user_id]

        elif state[0] == 'waiting_for_transfer':
            transfer_input = update.message.text.strip()
            sender_id = state[1]
            receiver_id = state[2]

            try:
                amount, currency = transfer_input.split()

                amount = float(amount)
                transfer_currency = currency.lower()

                await client_manager.confirm_transfer_message(update,amount, transfer_currency, sender_id, receiver_id)
                
                del user_input_state[user_id]  # Clear the state
            except ValueError as e:
                keyboard = [[InlineKeyboardButton("Back to Client list", callback_data='list_clients')]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(text= f"Invalid format. Please provide the amount to transfer in the format 'amount currency' (e.g., '5000 USDT'). Error: {str(e)}", 
                                                reply_markup=reply_markup)
            except Exception as e:
                await update.message.reply_text(f"An unexpected error occurred: {str(e)}")
        
#### -------------- Account History -----------------

async def show_account_history(query, client_id: int, offset: int ) -> None:
    summary, transaction_history = await transaction_management.get_account_history(client_id, offset)
    
    if summary is None:
        await query.edit_message_text("Client not found.")
        return

    transaction_history['transaction_date'] = pd.to_datetime(transaction_history['transaction_date']).dt.strftime('%d %b %H:%M')
    # Prepare transaction info
    if not transaction_history.empty:
        # Prepare a formatted string for the transactions
        transactions_info = "------------------------------------------------------------------------\n"
        transactions_info += "     *Date*         *Type*      *Currency*       *Size*\n"
        transactions_info += "------------------------------------------------------------------------\n"
        
        for index, row in transaction_history.iterrows():
            transactions_info += f"{row['transaction_date']}   {row['transaction_type']}       {row['transaction_currency']}        {int(row['transaction_size']):,}\n"

    else:
        transactions_info = "No transactions found."  

    # Create the new navigation buttons
    keyboard = []
    row = []
    
    if offset > 0:
        row.append(InlineKeyboardButton("Previous Transactions", callback_data=f'previous_transactions_{client_id}_{offset - 3}'))
    if offset + 3  == transaction_history.shape[0] + offset :
        row.append(InlineKeyboardButton("Next Transactions", callback_data=f'next_transactions_{client_id}_{offset + 3}'))    
    
    if row:
        keyboard.append(row)  # Add the row with buttons if it contains any buttons
    callback = 'list_clients'
    keyboard.append([InlineKeyboardButton("Back", callback_data=callback)])
  
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(f"{summary}\nLatest Transactions:\n{transactions_info}", reply_markup=reply_markup)


# ----------------- Order History ----------------- 

async def show_order_history(query, client_id: int, offset: int ) -> None:
    summary, order_history = await order_management.get_order_history(client_id,offset)
    if summary is None:
        await query.edit_message_text("Client not found.")
        return

    order_history['Order_date'] = pd.to_datetime(order_history['Order_date']).dt.strftime('%d %b %H:%M')
 
    # Prepare transaction info
    if not order_history.empty:
        # Prepare a formatted string for the transactions
        orders_info = "------------------------------------------------------------------------\n"
        orders_info +="   *Date*       *Type*  *Currency*   *Size*     *ExRate*\n"
        orders_info +="------------------------------------------------------------------------\n"
        
        for index, row in order_history.iterrows():
            orders_info += f"{row['Order_date']}   {row['Order_type']}       {row['Order_currency']}        {int(row['Order_size']):,}     {int(row['Order_price']):,}\n"

    else:
        orders_info = "No Order History for this client."

    # Create the new navigation buttons
    keyboard = []
    row = []
    
    if offset > 0:
        row.append(InlineKeyboardButton("Previous orders", callback_data=f'previous_orders_{client_id}_{offset - 3}'))
    if offset + 3  == order_history.shape[0] + offset :
        row.append(InlineKeyboardButton("Next orders", callback_data=f'next_orders_{client_id}_{offset + 3}'))    
    
    if row:
        keyboard.append(row)  # Add the row with buttons if it contains any buttons

    keyboard.append([InlineKeyboardButton("Edit Orders", callback_data=f'edit_orders_{client_id}')])  # New button for editing orders
    keyboard.append([InlineKeyboardButton("Back to Client list", callback_data='list_clients')])
  
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(f"{summary}\nLatest Orders:\n{orders_info}", reply_markup=reply_markup)


### ------------ CLIENT Details -----------------

async def show_client_details(query, client_id: int) -> None:
    details_message = await client_manager.show_client_details(client_id)  # Get client details from ClientManager
    
    
    keyboard = [
        
        [InlineKeyboardButton("New Order", callback_data=f'new_order_{client_id}'),
        InlineKeyboardButton("New Transaction", callback_data=f'new_transaction_{client_id}')],
        [InlineKeyboardButton("Order History", callback_data=f'order_history_{client_id}'),
        InlineKeyboardButton("Account History", callback_data=f'account_history_{client_id}')], 
        [InlineKeyboardButton("Edit Client", callback_data=f'edit_{client_id}'),
         InlineKeyboardButton("Transfer", callback_data=f'transfer_from_{client_id}')],
        [InlineKeyboardButton("Back to Clients list", callback_data='list_clients')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(details_message, reply_markup=reply_markup)  # Send the details message

### ------------ SEARCH -----------------

async def search_client(update: Update, query: str) -> None:
    await search_manager.get_search_results(update, query)

### --------------- InLine Query ------------- 

async def inline_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.inline_query.query.strip()

    # If query is empty, return no results
    if not query:
        return await update.inline_query.answer([])

    # Search for clients matching the query
    search_results = search_manager.search_clients(query.lower())
    
    # Create a list of InlineQueryResultArticle objects
    results = []
    for index, row in search_results.iterrows():
        client_name = f"{row['Client_name']} {row['Client_lastname']} (ID: {row['Client_id']})"
        
        # Create a simple inline query result showing the client name and ID
        results.append(
            InlineQueryResultArticle(
                id=str(row['Client_id']),  # Use client_id as a unique identifier
                title=client_name,
                input_message_content=InputTextMessageContent(
                    message_text=f"Client selected: {client_name}",
                ),
            )
        )

    await update.inline_query.answer(results)

### ------------- Google Sheet Upload ---------------

def scheduled_upload():
    global upload_thread 
    with upload_lock:
        logger.info("Scheduled upload started.")
        new_transaction_columns = ['DATE', 'Account ID', 'Order Ticket', 'Client Name', 'Type', 'Currency', 'Amount']
        new_hex_columns = ['DATE', 'Bought USDT', 'Sold USDT', 'Net Position']
        new_client_columns = ['Client ID', 'Account ID', 'First Name', 'USDT', 'TOMAN']
        new_order_columns = ['DATE', 'Account ID', 'Ticket', 'Client Name', 'Type', 'Currency',
                             'Size', 'ExchangeRate', 'Payable Toman', 'Status', 'Paid', 'Dept']

        if not os.path.exists(HEX_ACCOUNT_FILENAME):
            today_date = datetime.now().date().strftime("%Y-%m-%d") 
            instance_data = pd.DataFrame([[today_date, 0.0, 0.0, 0.0]],
                                          columns=['DATE', 'Bought_USDT', 'Sold_USDT', 'Net_position'])
            instance_data.to_csv(HEX_ACCOUNT_FILENAME, index=False)

        while True:
            today = datetime.now()  # Get the current date
            current_month_year = today.strftime("Report_%B_%Y")  # Get current month and year
            
            spreadsheet_id = google_manager.ensure_monthly_spreadsheet_exists(current_month_year)

            if not spreadsheet_id:
                logger.warning(f"No spreadsheet found for {current_month_year}. Please create it manually.")
                continue

            try:
                logger.info("Uploading CSV data to Google Sheets...")
                google_manager.upload_csv(HEX_ACCOUNT_FILENAME, 'Hex Dashboard', new_hex_columns)
                google_manager.upload_csv(CSV_FILENAME, 'Clients', new_client_columns)
                google_manager.upload_csv(ORDER_FILENAME, 'OrdersHistory', new_order_columns)
                google_manager.upload_csv(TRANSACTION_FILENAME, 'Transaction History', new_transaction_columns)

                google_manager.last_shared_month = google_manager.load_last_shared_month()

                # Check and share the spreadsheet if it's the first day of the month
                if today.day == 1 and google_manager.last_shared_month != current_month_year:
                    emails_to_share = ["your_email@gmail.com"]  # Specify recipient emails
                    google_manager.share_spreadsheet(spreadsheet_id, emails_to_share)
                    google_manager.save_last_shared_month(current_month_year)

                logger.info("Upload to Google Sheets completed successfully.")
            except Exception as e:
                logger.error(f"Error during scheduled upload: {str(e)}")
            
            time.sleep(60)  # Sleep for 1 minute

def start_upload_thread():
    global upload_thread 
    if upload_thread is None or not upload_thread.is_alive():
        upload_thread = threading.Thread(target=scheduled_upload)
        upload_thread.start()
        logger.info("Upload thread started.")
    else:
        logger.warning("Upload thread is already running.")



### ------------ MAIN ---------------

if __name__ == '__main__':
    print('Starting the bot...')
    app = Application.builder().token(TOKEN).build()

    # Add handlers
    app.add_handlers([ 
        CommandHandler('menu', menu),
        CommandHandler('search', search),
        CallbackQueryHandler(handle_button_click),
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message),
        InlineQueryHandler(inline_query_handler)
    ])

    

    print('Polling...')
    start_upload_thread()
    app.run_polling(poll_interval=3)
