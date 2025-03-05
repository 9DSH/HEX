
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext
from OrderFunctions import OrderManagement
from functions import ClientManager
from TransactionFunctions import TransactionManagement
from HexFunctions import HexAccountManager
import logging

logger = logging.getLogger(__name__)

class PaymentManager:
    def __init__(self, 
                 order_management: OrderManagement, 
                 client_manager: ClientManager , 
                 transaction_manager : TransactionManagement,
                 HexAccount_manager: HexAccountManager):
        
        self.HexAccount_manager = HexAccount_manager
        self.order_management = order_management
        self.client_manager = client_manager
        self.transaction_manager = transaction_manager

    async def confirm_new_order(self, update: Update,
                                client_id: int, 
                                order_input: str) -> None:
              
              
        try:
            order_type, order_size, order_currency, order_price = self.order_management.parse_order_input(order_input)
            
                         
            ex_type = 'Toman'
            
            user_name  = self.client_manager.get_name_by_client_id(client_id)
            summary_client_name = f"Account Name:     {user_name} \n"
         

            payment_due = float(order_size) * float(order_price)
            confirmation_text = (f"Confirm your order:\n\n"
                                 + summary_client_name+
                                  f"Type:                        {order_type}\n"
                                  f"Size:                          {int(order_size):,}\n"
                                  f"Currency:                {order_currency}\n"
                                  f"Exchange Rate:     {(order_price):,}\n\n"
                                  f"Amount Due ({ex_type}):     {int(payment_due):,}\n\n"
                                  "Click 'Complete Payment' to finalize the order, 'Enter Payment' for manual entry, or 'Cancel Order' to abort.")
         
            keyboard = [
                [
                    InlineKeyboardButton(
                        "Complete Payment",
                        callback_data=f'complete_payment_{client_id}_{order_type}_{order_size}_{order_currency}_{order_price}_{ex_type}'
                    ),
                    InlineKeyboardButton(
                        "Enter Payment",
                        callback_data=f'manual_{client_id}_{order_type}_{order_size}_{order_currency}_{order_price}_{payment_due}'
                    ),
                    InlineKeyboardButton(
                        "Pay Later",
                        callback_data=f'holding_payment_{client_id}_{order_type}_{order_size}_{order_currency}_{order_price}_{payment_due}'
                    )
                ],[ 
                    InlineKeyboardButton("Cancel Order", callback_data='cancel_order')
                  ]
            ]

            keyboard.append([InlineKeyboardButton("Back to Client list", callback_data='list_clients')])
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(confirmation_text, reply_markup=reply_markup)

        except ValueError as e:
            await update.message.reply_text(str(e))

    async def complete_payment(self, query: Update,  context: CallbackContext) -> None:
        parts = query.data.split('_')
        client_id = int(parts[2])  # Using the correct index for client_id
        order_type = parts[3]
        order_size = float(parts[4])
        order_currency = parts[5]
        order_price = float(parts[6])
        ex_type = parts[7]

        client_name = self.client_manager.get_name_by_client_id(client_id)
        # Transaction 
        transaction_currency = ex_type
        transaction_size = order_size * order_price

        if order_type == "BUY" :  transaction_type = 'Send'
        elif order_type == "SELL": transaction_type = 'Receive'

        status = 'Complete'
        
        paid_by_client = transaction_size
        
        order_ticket = self.order_management.create_order( client_id, order_type, order_currency, order_size, order_price, status, paid_by_client)
        update_net_position , summary = self.HexAccount_manager.Hex_summary()
        Completion_text = await self.transaction_manager.add_transaction(client_id,order_ticket,transaction_type,transaction_currency,transaction_size)

        await query.edit_message_text(f'New Order Created!\n\n'
                                       f'Client:                    {client_name}\n'
                                       f'Ticket:                    {order_ticket}\n'
                                       f'Type:                      {order_type}\n'
                                       f'Size:                        {int(order_size):,}\n'
                                       f'Currency:              {order_currency}\n'
                                       f'Exchange Rate:    {int(order_price):,}\n'
                                       f'Payment Due:      {int(transaction_size):,}\n'
                                       f'Order Status:       {status}')
        
        await context.bot.send_message(
                                        chat_id=query.message.chat_id,
                                        text=Completion_text,
                                        reply_to_message_id=query.message.message_id
                                      )
       
        
    async def confirm_manual_payment(self,
                                     update: Update,
                                     amount: float,         ### the amount user manually entered
                                      order_info) -> None:
        
        client_id = int(order_info[0])  # Using the correct index for client_id
        order_type = order_info[1]
        order_size = float(order_info[2])
        order_currency = order_info[3]
        order_price = float(order_info[4])
        payment_due = float(order_info[5])
             
        client_name = self.client_manager.get_name_by_client_id(client_id)

        # transaction details 
         
        
        confirm_message = (f'Confirm Manual Payment (Toman)!\n\n'
                          f'Client:                    {client_name}\n'
                          f'Payment Due:     {int(payment_due):,}\n'
                          f'Pay now:               {int(amount):,}\n')
       
        
       

            # Debug print statements for the callback_data components
        callback_data_manual_payment = f'C_manual_{client_id}_{order_type}_{order_size}_{order_currency}_{order_price}_{payment_due}_{amount}'
         
        keyboard = [
            [InlineKeyboardButton("Confirm", callback_data=callback_data_manual_payment),
            InlineKeyboardButton("Cancel", callback_data='list_clients')]
             
        ]    
          
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
                                        text=confirm_message,
                                        reply_markup= reply_markup

                                    ) 

        


    async def process_payment_amount(self,
                                     query: Update,
                                     amount: float,         ### the amount user manually entered
                                     order_info) -> None:
        
        client_id = int(order_info[0])  # Using the correct index for client_id
        order_type = order_info[1]
        order_size = float(order_info[2])
        order_currency = order_info[3]
        order_price = float(order_info[4])
        payment_due = float(order_info[5])
             
        client_name = self.client_manager.get_name_by_client_id(client_id)

        # transaction details 
        
        transaction_currency = 'Toman'   
        actual_amount = payment_due  - amount   

        if order_type == "BUY" :  transaction_type = 'Send'   
        elif order_type == "SELL" : transaction_type = 'Receive'  

        
        status = 'Manual'
        paid_by_client = amount

        self.transaction_manager.update_client_balance(client_id, transaction_type , transaction_currency, actual_amount)
        order_ticket = self.order_management.create_order(client_id, order_type, order_currency, order_size, order_price, status, paid_by_client)
        Completion_text = await self.transaction_manager.add_transaction(client_id,order_ticket,transaction_type,transaction_currency, amount)
        update_net_position , summary = self.HexAccount_manager.Hex_summary()
        
        order_message = await query.edit_message_text(f'New Order Created!\n\n'
                                       f'Client:                    {client_name}\n'
                                       f'Ticket:                    {order_ticket}\n'
                                       f'Type:                      {order_type}\n'
                                       f'Size:                        {int(order_size):,}\n'
                                       f'Currency:              {order_currency}\n'
                                       f'Exchange Rate:    {int(order_price):,}\n'
                                       f'Payment Due:      {int(payment_due):,}\n'
                                       f'Order Status:       {status}')
        

        await query.message.reply_text(
                                        text=Completion_text,
                                        reply_to_message_id=order_message.message_id
                                    ) 
                
    async def process_holding_payment(self,
                           query: Update,  context: CallbackContext) -> None:       
        
        parts = query.data.split('_')
        client_id = int(parts[2])  # Using the correct index for client_id
        order_type = parts[3]
        order_size = float(parts[4])
        order_currency = parts[5]
        order_price = float(parts[6])
        payment_due = float(parts[7])


        
        transaction_currency = 'Toman'
        actual_amount =  payment_due # 100,000,000 

        if order_type == "BUY" :  transaction_type = 'Send'    
        elif order_type == "SELL" :  transaction_type = 'Receive'

        client_name = self.client_manager.get_name_by_client_id(client_id)
        Completion_text = (
                f"Ticket Summary:\n"
                f"Amount Due ({transaction_currency}): {int(payment_due):,}\n"
              )
    
        status = 'Pending'
        paid_by_client = 0.0
        self.transaction_manager.update_client_balance(client_id, transaction_type , transaction_currency, actual_amount)
        
        order_ticket = self.order_management.create_order(client_id, order_type, order_currency, order_size, order_price, status, paid_by_client)
        update_net_position , summary = self.HexAccount_manager.Hex_summary()
    

        await query.edit_message_text(f'New Order Created!\n\n'
                                       f'Client:                    {client_name}\n'
                                       f'Ticket:                    {order_ticket}\n'
                                       f'Type:                      {order_type}\n'
                                       f'Size:                        {int(order_size):,}\n'
                                       f'Currency:              {order_currency}\n'
                                       f'Exchange Rate:    {int(order_price):,}\n'
                                       f'Payment Due:      {int(payment_due):,}\n'
                                       f'Order Status:       {status}')
        
        await context.bot.send_message(
                                        chat_id=query.message.chat_id,
                                        text=Completion_text,
                                        reply_to_message_id=query.message.message_id
                                      )
        

    async def handle_cancel_order(self, update: Update, context) -> None:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("Order has been canceled.")