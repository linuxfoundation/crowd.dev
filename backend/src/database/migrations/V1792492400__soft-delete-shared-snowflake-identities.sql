with deleted as (
    update "memberIdentities"
    set "deletedAt" = now(),
        "updatedAt" = now()
    where "deletedAt" is null
      and (
          (
              platform = 'meetings'
              and type in ('email', 'username')
              and lower(value) in (
                  'meeting1@seidengroup.com',
                  'meetings@dpdk.org',
                  'zoom-admin@nephio.org',
                  'zoom0@hyperledger.org',
                  'zoom2@hyperledger.org',
                  'zoom2@lfai.foundation',
                  'zoom@hyperledger.org',
                  'zoom@longnowboston.org',
                  'zoom@openwallet.foundation'
              )
          )
          or (platform = 'cvent' and type = 'username' and lower(value) in ('na', 'none', 'null'))
          or (platform = 'lfid' and type = 'username' and lower(value) = 'undefined')
      )
    returning "memberId"
)
update members
set "updatedAt" = now()
where id in (select "memberId" from deleted);
