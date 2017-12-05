


create function handle_calc

	  (@part1 double, @operator varchar(1), @part2 double)

	  returns double

as



    IF	@operator not in ('+','-','*','/')
      BEGIN

	       raiserror 20000 "Invalid value for @operator, must be either: < + - * / > "

	       return

      END

    IF @operator = '+'
      BEGIN
        return @part1 + @part2
      END
    ELSE IF @operator = '-'
      BEGIN
        return @part1 - @part2
      END
    ELSE IF @operator = '*'
      BEGIN
        return @part1 * @part2
      END
    ELSE IF @operator = '/'
      BEGIN
        if (@part2 = 0) return 0
        return @part1 / @part2
      END
--    ELSE
return  null


