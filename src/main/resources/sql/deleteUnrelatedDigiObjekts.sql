delete from DIGOBJEKT
where ID not in (
    select KAM from DIGVAZBY
)