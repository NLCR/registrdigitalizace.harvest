insert into DIGMETADATA_CHANGES 
select DIGOBJEKTID, sum(MODIFIKACE) as MODIFIKACE from (
    select CONNECT_BY_ROOT O.ID as DIGOBJEKTID, MODIFIKACE
        from DIGVAZBY V, DIGOBJEKT O, DIGMETADATA M
        where V.POTOMEK = O.UUID and O.ID = M.ID
        start with O.ID in (select RDIGOBJEKTL from LOKACE)
        connect by prior V.PREDEK = O.ID
) group by DIGOBJEKTID
