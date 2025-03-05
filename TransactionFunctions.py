import pandas as pd
import random
import os
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
import logging
from functions import ClientManager
from HexFunctions import HexAccountManager

# Logging configuration
logger = logging.getLogger(__name__)

class TransactionManagement:
    def __init__(self,
                 csv_filename: str ,
                 client_manager: ClientManager ,
                 HexAccount_Manager: HexAccountManager):
        
        self.csv_filename = csv_filename
        self.client_manager = client_manager
        self.HexAccount_Manager = HexAccount_Manager
        self.df = self.load_data()

    def load_data(self):
        """Load transaction data from CSV or create an empty DataFrame if the file does not exist."""
        if os.path.exists(self.csv_filename):
            try:
                df = pd.read_csv(self.csv_filename)
                logger.info("Transaction data loaded successfully.")
                return df
            except Exception as e:
                logger.error(f"Error reading Transactions CSV file: {str(e)}")
                return pd.DataFrame(columns=['transaction_date','Account ID', 'Order Ticket', 'Client_name', 'transaction_type',
                                             'transaction_currency', 'transaction_size'])
        else:
            logger.info("Transactions CSV file not found. Creating a new one.")
            return pd.DataFrame(columns=['transaction_date','Account ID', 'Order Ticket', 'Client_name', 'transaction_type',
                                         'transaction_currency', 'transaction_size'])

    def save_data(self):
        try:
            self.df.to_csv(self.csv_filename, index=False) 
            
            logger.info(f"Transaction data saved successfully to {self.csv_filename}.")
        except Exception as e:
            logger.error(f"Error saving Transactions CSV file: {str(e)}")


    def remove_transaction(self, order_ticket: int):
        try:
            # Find rows where the order_ticket matches
            matching_rows = self.df[self.df['Order Ticket'] == order_ticket]

            if matching_rows.empty:
                logger.warning("No matching transactions found for the given order ticket.")
                return False  # No matching rows found

            # Remove the matching rows and update self.df
            self.df = self.df.drop(self.df[self.df['Order Ticket'] == order_ticket].index)

            # Save updated DataFrame to CSV
            self.save_data()  # Call save_data() to persist changes

            logger.info(f"Removed {len(matching_rows)} transaction(s) and saved changes.")
            return True
        except Exception as e:
            logger.error(f"Error removing transaction: {str(e)}")
            return False




    def update_transaction(self, previous_payable_to_toman: float, new_payable_to_toman: float) -> bool:
        """Update the 'payable_to_Toman' value for transactions matching the previous payable amount."""
        try:
            # Find rows where the payable_to_Toman matches the provided previous value
            matching_rows = self.df[self.df['transaction_size'] == previous_payable_to_toman]

            if matching_rows.empty:
                logger.warning("No matching transactions found for the given previous payable to Toman.")
                return False  # No matching rows found

            # Update the matching rows with the new payable to Toman value
            self.df.loc[self.df['transaction_size'] == previous_payable_to_toman, 'transaction_size'] = new_payable_to_toman
            
            # Save the updated DataFrame to CSV
            self.save_data()  
            logger.info(f"Updated {len(matching_rows)} transactions from {previous_payable_to_toman} to {new_payable_to_toman}.")
            return True
        except Exception as e:
            logger.error(f"Error updating transaction payable to Toman: {str(e)}")
            return False

    async def confirm_new_transaction(self,update: Update, client_id: int, transaction_input) -> None:

        user_name = self.client_manager.get_name_by_client_id(client_id)
        summary_client_name = f"Account Name:    {user_name} \n"
        
        try:
            transaction_type, transaction_size, transaction_currency = self.parse_transaction_input(transaction_input)

            confirmation_text = (f"Confirm your transaction:\n\n"
                                 +summary_client_name+
                                 f"Client ID:                {client_id}\n"
                                 f"Type:                       {transaction_type}\n"
                                 f"Size:                         {int(transaction_size):,}\n"
                                 f"Currency:               {transaction_currency}\n\n"
                                  "Click 'Complete transaction' to finalize the transaction,  or 'Cancel ' to abort.")

            keyboard = [
                [InlineKeyboardButton("Complete Transaction", callback_data=f'complete_transaction_{client_id}_{transaction_type}_{transaction_size}_{transaction_currency}'),  
                 InlineKeyboardButton("Cancel", callback_data='cancel_transaction_')]
            ]
            keyboard.append( [InlineKeyboardButton("Back to Client list", callback_data='list_clients')])
            
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(confirmation_text, reply_markup=reply_markup)

        except ValueError as e:
            await update.message.reply_text(str(e))

    async def add_transaction(self, client_id: int, order_ticket: int,transaction_type: str, transaction_currency: str, transaction_size: float) -> None:
   
        transaction_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        transaction_currency = transaction_currency.upper() 
        transaction_type = transaction_type.capitalize()
        client_name = self.client_manager.get_client_name(client_id)
        
       
        account_id , Completion_text = self.format_transaction(client_id, transaction_type, transaction_size, transaction_currency)

        new_transaction = pd.DataFrame([[transaction_date, account_id, order_ticket, client_name, transaction_type,
                                          transaction_currency, transaction_size]],
                                        columns=['transaction_date','Account ID', 'Order Ticket', 'Client_name', 'transaction_type',
                                                 'transaction_currency', 'transaction_size'])
        
                           
        self.df = pd.concat([self.df, new_transaction], ignore_index=True)
        self.save_data()  # Save after adding transaction    


        return Completion_text


    def format_transaction(self, client_id,transaction_type, transaction_size, transaction_currency) :
        
        transaction_currency = transaction_currency.upper()
        client_info = self.client_manager.get_client_details(client_id)
        account_id = self.client_manager.get_account_id_by_client_id(client_id)
        

        Completion_text = (
                          f"Client Name:      {client_info['Client_name']}\n"
                          f"-------------------------------------------------------------------------\n"
                          f"                        Transaction successful \n"
                          f"-------------------------------------------------------------------------\n"
                          f"Type:                    {transaction_type.capitalize()}\n"
                          f"Size:                      {int(transaction_size):,}\n"
                          f"Currency:            {transaction_currency.capitalize()}\n"
                          f"-------------------------------------------------------------------------\n"
                          f"                           Updated Balances\n"
                          f"-------------------------------------------------------------------------\n"
                          f"Balance (USDT):                         {int(client_info['USDT_Balance']):,}\n"
                          f"Balance (TOMAN):                    {int(client_info['Toman_Balance']):,}\n\n"
            )
            

        return  account_id , Completion_text


    def update_client_balance(self,client_id: int, transaction_type: str,  transaction_currency: str, amount: float):   ### in manual payment amount = abstract

        client_info = self.client_manager.get_client_details(client_id)
        transaction_currency = transaction_currency.upper()
        
        if not client_id:  # Check if client_info is None
            logger.error(f"Client ID {client_id} not found when trying to update balance.")
            raise ValueError(f"Client ID {client_id} not found.")

        # Update client USDT balance
        if transaction_currency == 'USDT':
            current_balance = client_info['USDT_Balance']
            if transaction_type == "Send" : new_balance = current_balance + amount
            elif transaction_type == "Receive" : new_balance = current_balance - amount

            self.client_manager.edit_client(client_id, new_usdt_balance=new_balance) 
            
        # Update client Toman balance
        elif transaction_currency == 'TOMAN':
            current_balance = client_info['Toman_Balance']
            if transaction_type == "Send" : 
                new_balance = current_balance + amount
            elif transaction_type == "Receive" : 
                new_balance = current_balance - amount

            self.client_manager.edit_client(client_id, new_toman_balance=new_balance) 
            

    async def get_account_history(self, client_id: int , offset: int):
        """Retrieve summary and latest transactions for the given client ID."""
        account_id = self.client_manager.get_account_id_by_client_id(client_id)
        client_info = self.client_manager.get_client_details(client_id) 
        summary_client_name = f"Account Name:      {client_info['Client_name']}\n"
        
        total_toman_deposit,total_usdt_deposit,total_toman_withdraw, total_usdt_withdraw = self.get_totals_for_today(account_id)
        
        latest_transactions = self.get_latest_transactions(account_id,offset)

        if client_info is None:
            return None, None

        summary = (
             summary_client_name +
            f"Account ID:              {account_id}\n"
            f"-------------------------------------------------------------------------\n"
            f"Balance (USDT):                       {int(client_info['USDT_Balance']):,}\n"
            f"Balance (TOMAN):                  {int(client_info['Toman_Balance']):,}\n\n"
            f"-------------------------------------------------------------------------\n"
            f"                       Total Sent (Today)\n"  
            f"-------------------------------------------------------------------------\n"    
            f"Toman:                       {int(total_toman_deposit):,}\n"
            f"USDT:                          {int(total_usdt_deposit):,}\n"
            f"-------------------------------------------------------------------------\n"
            f"                       Total Received (Today)\n"     
            f"-------------------------------------------------------------------------\n"    
            f"Toman:                       {int(total_toman_withdraw):,}\n"
            f"USDT:                          {int(total_usdt_withdraw):,}\n"
        )

        return summary, latest_transactions 

    def parse_transaction_input(self, transaction_input: str):
        parts = transaction_input.strip().split()

        if len(parts) != 3:
            raise ValueError('Invalid format. Please start over and use this format: "deposit|withdraw 5000 USDT".')

        transaction_type = parts[0].lower()
        if transaction_type in ['s', 'send']:
            transaction_type = 'Send'
        elif transaction_type in ['r', 'receive']:
            transaction_type = 'Receive'
        else:
            raise ValueError('Transaction type must be either "deposit" | "d" or "withdraw" | "w".')

        try:
            transaction_size = float(parts[1])
            if transaction_size <= 0:
                raise ValueError('Transaction size must be greater than zero.')
        except ValueError:
            raise ValueError('Transaction size must be a numeric value.')

        transaction_currency = parts[2].lower()
        transaction_currency = transaction_currency.upper()
        return transaction_type, transaction_size, transaction_currency
    

    async def handle_cancel_transaction(self, update: Update, context) -> None:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("Transaction has been canceled.")

    def get_totals_for_today(self, account_id: int):

        today = datetime.now().date().strftime("%Y-%m-%d")
        transactions_today_usdt = self.df[self.df['transaction_date'].str.startswith(today) & 
                                    (self.df['Account ID'] == account_id) &
                                    (self.df['transaction_currency'] == 'USDT')]
        
        transactions_today_toman = self.df[self.df['transaction_date'].str.startswith(today) & 
                                    (self.df['Account ID'] == account_id) &
                                    (self.df['transaction_currency'] == 'TOMAN')]
        

        total_usdt_deposit = transactions_today_usdt[transactions_today_usdt['transaction_type'] == 'Send']['transaction_size'].sum()
        total_usdt_withdraw = transactions_today_usdt[transactions_today_usdt['transaction_type'] == 'Receive']['transaction_size'].sum()

        total_toman_deposit = transactions_today_toman[transactions_today_toman['transaction_type'] == 'Send']['transaction_size'].sum()
        total_toman_withdraw = transactions_today_toman[transactions_today_toman['transaction_type'] == 'Receive']['transaction_size'].sum()

      
        
        return total_toman_deposit , total_usdt_deposit, total_toman_withdraw, total_usdt_withdraw

    def get_latest_transactions(self, account_id: int, offset: int):
        transactions = self.df[self.df['Account ID'] == account_id]
        transactions = transactions[['transaction_date', 'transaction_type', 'transaction_currency', 'transaction_size']]

       
        sorted_transactions = transactions.sort_values(by='transaction_date', ascending=False)
        latest_transactions = sorted_transactions.iloc[offset:offset + 3]
      
        return latest_transactions 
    
    async def get_all_account_history(self, client_id: int):
        """Retrieve transaction history for a given account ID."""
        account_id = self.client_manager.get_account_id_by_client_id(client_id)
        return self.df[self.df['Account ID'] == account_id]  # Return transaction history for the account ID
    

    