SQL_STATEMENTS = [
    
    { 'TYPE' : "INSERT" ,   'STATEMENT'  : """
INSERT INTO xxaih_pos_so_import 
(OUID, ORGID, ORDERID, ORDERPROVNO, ORDERDATE, ORDERTYPE, CASHCREDIT, CUSTOMERID, CUSTNAME,
CUSTADDRESS, CUSTTEL, ORDERTOTAL, EMPLOYEEID, LINEID, LINESEQ, ITEMID, ITEMCODE, SHIPFROMORG, SUBINVENTORY, LINETYPE,
BARCODE, QUANTITY, UOMCODE, ITEMSERIAL, DISCOUNTPER, DISCOUNTAMT, LISTPRICE, SELLINGPRICE, COMMISSIONVALUE,
SALESPERSONID, SERVICETAX, VAT, SALESTAX, CORPORATEDISC, PROMOTIONID, ADDDATE, ERPFLAG, DELIVERYDATE)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""} , 
                 
            ]