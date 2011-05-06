insert into LOKACE
(ID, DIGKNIHOVNA, DATUMZAL, RDIGOBJEKTL)
select ?, DL."VALUE", ?, ? from DLISTS DL where DL.ID=?