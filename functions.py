import pandas as pd
import random
import os
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update


# Logging configuration
logger = logging.getLogger(__name__)

class ClientManager:
    def __init__(self,csv_filename: str):
        self.csv_filename = csv_filename
        self.df = self.load_data()

    def load_data(self):
        try:
            if os.path.exists(self.csv_filename):
                return pd.read_csv(self.csv_filename)
            else:
                return pd.DataFrame(columns=[
                    'Client_id', 'account_id', 'Client_name',
                     'USDT_Balance', 'Toman_Balance'
                ])
        except Exception as e:
            logger.error(f"Error reading CSV file: {str(e)}")
            return pd.DataFrame(columns=[
                'Client_id', 'account_id', 'Client_name', 
               'USDT_Balance', 'Toman_Balance'
            ])

    def save_data(self):
        # Now all updates should happen in Google Sheets directly
        try:
            self.df.to_csv(self.csv_filename, index=False)
            # Rebuild the DataFrame in case it's not correct after updates
            
        except Exception as e:
            logger.error(f"Error updating Google Sheets: {str(e)}")
            return False
        return True

    def add_client(self, client_name: str, 
                   usdt_balance: float = 0.0,
                   toman_balance: float = 0.0):
        
        existing_client = self.df[(self.df['Client_name'].str.lower() == client_name.lower())]

        if not existing_client.empty:
            return None  # Client already exists

        while True:
            client_id = random.randint(1000, 9999)
            if client_id not in self.df['Client_id'].values:
                break
        
        account_id = random.randint(1000, 9999)
        
        new_client = pd.DataFrame([[client_id, account_id, client_name,
                                    usdt_balance, toman_balance]],
                                   columns=['Client_id', 'account_id', 'Client_name',
                                             'USDT_Balance','Toman_Balance'])

        self.df = pd.concat([self.df, new_client], ignore_index=True)
        self.save_data()
        return client_id  # Return new client ID
    
    def edit_client(self, client_id: int, new_name: str = None, new_toman_balance: float = None, new_usdt_balance: float = None):
        client_row = self.df[self.df['Client_id'] == client_id]
        
        if not client_row.empty: 
            if new_name:
                self.df.loc[self.df['Client_id'] == client_id, 'Client_name'] = new_name
            if new_toman_balance is not None:
                self.df.loc[self.df['Client_id'] == client_id, 'Toman_Balance'] = new_toman_balance
            if new_usdt_balance is not None:
                self.df.loc[self.df['Client_id'] == client_id, 'USDT_Balance'] = new_usdt_balance
                
            self.save_data()
            return self.df.loc[self.df['Client_id'] == client_id].iloc[0]
        return None

    def get_all_clients(self):
        return self.df[['Client_id', 'Client_name']].to_dict(orient='records')

    async def list_clients(self, query, offset=0) -> None:
        clients_info = self.df[['Client_id', 'account_id', 'Client_name', 
                                'USDT_Balance', 'Toman_Balance']] 

        total_clients = len(clients_info)
        clients_per_page = 5  # Set the number of clients per page
        clients_to_display = clients_info.iloc[offset:offset + clients_per_page]

        if clients_to_display.empty:
            keyboard = []
            keyboard.append([InlineKeyboardButton("Add New Client", callback_data='add_new_client'),
                            InlineKeyboardButton("Back to Main Menu", callback_data='back_to_main_menu')])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text('No clients found.', reply_markup=reply_markup)
        else:
            keyboard = [
                [InlineKeyboardButton(f'{row["Client_name"]} || {int(row["Toman_Balance"]):,}', 
                callback_data=str(row['Client_id']))] for _, row in clients_to_display.iterrows()
            ]
            
            # Create the pagination buttons
            nav_buttons = []
            if offset > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f'clients_page_{offset - clients_per_page}'))
            if offset + clients_per_page < total_clients:
                nav_buttons.append(InlineKeyboardButton("➡️ Next", callback_data=f'clients_page_{offset + clients_per_page}'))
            
            keyboard.append(nav_buttons)
            keyboard.append([InlineKeyboardButton("Add New Client", callback_data='add_new_client'),
                            InlineKeyboardButton("Back to Main Menu", callback_data='back_to_main_menu')])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(f'Current clients: {total_clients}', reply_markup=reply_markup)
    
    def get_client_details(self, client_id: int):
        client_details = self.df[self.df['Client_id'] == client_id]
        if not client_details.empty:
            return client_details.iloc[0]
        return None

    def format_client_details(self, client_info):
        return (
                f"Account Name:  {client_info['Client_name']} \n"
                f"---------------------------------------------------------------------------\n"
                f"Balance (USDT):                   {int(client_info['USDT_Balance']):,}\n"
                f"Balance (TOMAN):               {int(client_info['Toman_Balance']):,}\n"
        )

    async def show_client_details(self, client_id: int) -> None:
        client_info = self.get_client_details(client_id)
        if client_info is not None:
            return self.format_client_details(client_info)
        return 'Client not found.'

    async def present_edit_options(self, query, client_id: int) -> None: 
        keyboard = [
        [InlineKeyboardButton("Edit Name", callback_data=f'edit_name_{client_id}')],
        [InlineKeyboardButton("Back to Main Menu", callback_data='back_to_main_menu')]
       ]
       
        reply_markup = InlineKeyboardMarkup(keyboard)   
        await query.edit_message_text("What would you like to edit?", reply_markup=reply_markup)

    async def edit_client_name(self, query, client_id: int, input_state) -> None: 
        await query.edit_message_text('Please provide the new name:')
        input_state[query.from_user.id] = ('waiting_for_edit_name', client_id)


    async def handle_edit_name(self, client_id: int, new_name: str):
        updated_info = self.edit_client(client_id, new_name=new_name)
        if updated_info is not None:
            return f'Client updated successfully:\n\n' \
                   f"Name:          {updated_info['Client_name']}\n" \
                   f"Client ID:     {updated_info['Client_id']}\n" \
                   f"Account ID: {updated_info['account_id']}\n\n" \
                   f"----------------------------------------------\n" \
                   f"Balance (USDT):                  {int(updated_info['USDT_Balance']):,}\n"  \
                   f"Balance (TOMAN):              {int(updated_info['Toman_Balance']):,}\n" 
                   
        return 'Client not found.'

    def get_client_name(self, client_id: int) -> str:
        """Retrieve the Client name based on the given Client ID. """
        client_row = self.df[self.df['Client_id'] == client_id]
        if not client_row.empty:
            return client_row['Client_name'].values[0]  # Return the first matching client ID
        return None  # Return None if the account ID doesn't match any client
    
    def get_client_id_by_account_id(self, account_id: int) -> int:
        """Retrieve the Client ID based on the given Client Account ID. """

        client_row = self.df[self.df['account_id'] == account_id]
        if not client_row.empty:
            return client_row['Client_id'].values[0]  # Return the first matching client ID
        return None  # Return None if the account ID doesn't match any client
    
    def get_account_id_by_client_id(self, client_id: int) -> int:        
        client_row = self.df[self.df['Client_id'] == client_id]
        if not client_row.empty:
            return client_row['account_id'].values[0] 
        return None  
    
    def get_name_by_client_id(self, client_id: int) -> int:        
        client_row = self.df[self.df['Client_id'] == client_id]
        if not client_row.empty:
            name = client_row['Client_name'].values[0] 
            return name
        return None  
    
    #---------- Transfer ----------

    def get_all_accounts_for_Transfer(self):
        return self.df[['Client_id','account_id',
                         'Client_name','USDT_Balance',
                         'Toman_Balance']].to_dict(orient='records')
    
    async def handle_transfer(self, query, from_client_id: int) -> None:
        available_accounts = self.get_all_accounts_for_Transfer()  # Get all Hex accounts
        
        keyboard = [
            [InlineKeyboardButton(f'{row["Client_name"]}', callback_data=f'transfer_to_{row["Client_id"]}_{from_client_id}') for row in available_accounts if row['Client_id'] != from_client_id]
        ]
        keyboard.append([
            InlineKeyboardButton("Cancel", callback_data='list_clients')]
                  )  
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text("Select the account to transfer to:", reply_markup=reply_markup)

    def get_client_id_by_name(self, client_name: str) -> int:
        """Retrieve the Client ID based on the given Client name."""
        client_row = self.df[self.df['Client_name'].str.lower() == client_name.lower()]
        if not client_row.empty:
            return client_row['Client_id'].values[0]  # Return the first matching client ID
        return None  # Return None if the client name doesn't match any client
    
    async def confirm_transfer_message(self, update: Update, amount: float, currency: str, sender_id: int, receiver_id: int ) -> None:
        try:
            
            # Retrieve sender and receiver client IDs using their names
            sender_name = self.get_client_name(sender_id)
            receiver_name = self.get_client_name(receiver_id)

            if sender_id is None or receiver_id is None:
                raise ValueError("Sender or receiver not found.")

            currency = currency.upper()

            confirmation_message = (
                                    f"Confirm Transfer:\n\n"
                                    f"Amount:    {int(amount):,} \n"
                                    f"Currency:   {currency}\n"
                                    f"From:          {sender_name}\n"
                                    f"To:               {receiver_name}\n"
                                    )

            keyboard = [
                [InlineKeyboardButton("Confirm", callback_data=f'confirm_transfer_{amount}_{currency}_{sender_id}_{receiver_id}'),
                InlineKeyboardButton("Cancel", callback_data='back_to_main_menu')]
                ]
               
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(confirmation_message, reply_markup=reply_markup)


            

        except ValueError as e:
            await update.message.reply_text(f"Error: {str(e)}")
        except Exception as e:
            await update.message.reply_text(f"An unexpected error occurred: {str(e)}")


    
    
class SearchManager:
    def __init__(self, client_manager: ClientManager):
        self.client_manager = client_manager

    def search_clients(self, query: str):
        results = self.client_manager.df[(
            self.client_manager.df['Client_name'].str.lower().str.contains(query)) | 
            (self.client_manager.df['Client_id'].astype(str).str.contains(query)) |
            (self.client_manager.df['account_id'].astype(str).str.contains(query))
        ]
        return results

    async def get_search_results(self, update, query: str):
        results = self.search_clients(query)
        if results.empty:
            await update.message.reply_text('No clients found matching your search.')
        else:
            keyboard = [
                [InlineKeyboardButton(f'{row["Client_name"]}', 
                                    callback_data=str(row['Client_id']))] for _, row in results.iterrows()
            ]
            
            keyboard.append([InlineKeyboardButton("See all Clients", callback_data='list_clients'),
                             InlineKeyboardButton("Back to Main Menu", callback_data='back_to_main_menu')])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text('Clients found matching your search:', reply_markup=reply_markup)





 