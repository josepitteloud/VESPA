
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################
-- ####################################################################################################


/******************************************************************************
**
** Project Vespa: Metric Benchmark Function
**
** This script has the function which we use to compare result to the metric amber and metric red
** benchmark values so we can assign a red, amber or green status.
**
** Refer also to:
**
**
**      Part A: Function details
**      A01 - Housekeeping
**      A02 - Mathematical function to gain results
**      A03 - Logic to decide what the RAG status will be
**      A04 - return result
**
**
** Things done:
**
**
******************************************************************************/


if object_id('metric_benchmark_check') is not null drop function metric_benchmark_check
commit

go

create function metric_benchmark_check(
  @metric_result decimal(16,3),
  @metric_benchmark decimal(16,3),
  @metric_tolerance_amber decimal(16,3),@metric_tolerance_red decimal(16,3) )
returns varchar(8)
as
begin

  declare @benchmark_result varchar(8)
  declare @result_benchmark decimal(16,3)
  declare @metric_benchmark_divide decimal(16,3)


-----------------------------------------------------------A01 - Housekeeping-----------------------------------------------------

  if @metric_benchmark = 0
    begin
      set @metric_benchmark_divide = 1
    end
  if @metric_benchmark > 0
    begin
      set @metric_benchmark_divide = @metric_benchmark
    end

-----------------------------------------------------------A02 - Mathematical function to gain results--------------------------------------

  set @result_benchmark = abs(1.0*(@metric_result-@metric_benchmark)/@metric_benchmark_divide)*100

-----------------------------------------------------------A03 - Logic to decide what the RAG status will be--------------------------------------

  if(@result_benchmark <= @metric_tolerance_amber)
    begin
      set @benchmark_result = 'GREEN'
    end
  if(@result_benchmark between(@metric_tolerance_amber+.001) and(@metric_tolerance_red-.001))
    begin
      set @benchmark_result = 'AMBER'
    end
  if(@result_benchmark >= @metric_tolerance_red)
    begin
      set @benchmark_result = 'RED'
    end

-----------------------------------------------------------A04 - return result--------------------------------------

  return @benchmark_result

end

go

grant execute on metric_benchmark_check to sk_prodreg