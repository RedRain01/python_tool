from dataclasses import dataclass


@dataclass
class Ticket:
    def __init__(self,id: str, order_date: str, order_time: str, order_price: float, order_change: float,
                 order_volume: int, order_amount: int, order_type: str,
                 order_code: str,order_symbol: str,ticket_name:str,ticket_area:str,
                 industry:str,list_date:str,page_num:int,page_var:str):
        self.id = id
        self.order_date = order_date
        self.order_time = order_time
        self.order_price = order_price
        self.order_change = order_change
        self.order_volume = order_volume
        self.order_amount = order_amount
        self.order_type = order_type
        self.order_code = order_code
        self.order_symbol = order_symbol
        self.ticket_name = ticket_name
        self.ticket_area = ticket_area
        self.industry = industry
        self.list_date = list_date
        self.page_num = page_num
        self.page_var = page_var
        


