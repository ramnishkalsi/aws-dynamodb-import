create or replace view v_state as 
Select distinct state.state_id, state.state_name, state.state_desc, count(hotel.hotel_id) as hotel_count
from  state_table state, hotel_address ha, hotel hotel
where  
state.state_id = ha.state_table_state_id
and ha.address_id = hotel.hotel_address_address_id
and hotel.status_code  IN (1,2,100,101) 
group by 1 
order by state.state_name
;
