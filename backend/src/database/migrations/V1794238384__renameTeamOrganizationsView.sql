update "customViews"
set name = 'Team companies', "updatedAt" = now()
where name = 'Team organizations'
  and placement = 'organization'
  and visibility = 'tenant';
