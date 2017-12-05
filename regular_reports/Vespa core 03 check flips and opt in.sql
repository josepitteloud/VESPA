-- Project Vespa Core Control 2: check if tables have flipped and if we're ready to process this week

if varexists('@flips_happened') = 0 then
    create variable @flips_happened bit end if;
if varexists('@opt_in') = 0 then
    create variable @opt_in         bit end if;

execute('CITeam.VES024_Check_Flips @flips_happened');
execute('CITeam.VES024_Check_Opt_In @opt_in');

select
    @flips_happened as Flips_happened
    ,@opt_in as opted_in
; -- We line them up in the Report Suite
