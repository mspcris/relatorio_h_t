Declare @DataInicio date = :ini;
Declare @DataFinal date = :fim; -- Ultimo Dia + 1
with

tabDatas as (
select distinct e.Especialidade, d.data 
from sis_datahora d 
cross join Cad_Especialidade e 
where cast(d.Data as date) >= @DataInicio
and cast(d.Data as date) < @DataFinal
and e.Desativado = 0
),
tabVagas as (
--  tabVagas = Total de vagas 
select          
count(n.numero) as Quantidade, 
d.Data as Data,          
e.Especialidade

from sis_numero n          
join sis_datahora d on cast(d.Data as date) >= @DataInicio
join cad_especialidade e on          
(      
      
 ((          
  isnull(e.DataInicioExibicao, @DataFinal )  < @DataFinal
  and isnull(e.DataFimExibicao, @DataInicio ) >= @DataInicio
  and e.Desativado = 0          
 )          
          
-- Adicionado em 29/09/2020 para que site e APP respeitem a data final de exibição do médico          
and (IsNull(e.DataFimExibicao, d.data) >= d.Data)          
          
 )          
 or (e.DataPlantao is not null and e.desativado =0)          
)      
-- reserva      
and      
(      
  ( (datepart(dw, d.data) = 1))
 or      
  ( (datepart(dw, d.data) = 2))
 or      
  ( (datepart(dw, d.data) = 3))
 or      
  ( (datepart(dw, d.data) = 4))
 or      
  ( (datepart(dw, d.data) = 5))
 or      
  ( (datepart(dw, d.data) = 6))
 or      
  ( (datepart(dw, d.data) = 7))      
)      

join Cad_Medico m on m.idmedico = e.idmedico and m.Desativado =0          
left join vw_Cad_EspecialidadeQuantidadeAteAlmoco a on a.idespecialidade = e.idEspecialidade and a.data is not null and cast( a.data as datetime) = d.Data
left join Cad_MedicoFalta mf on mf.idMedico = m.idMedico 
	and cast(mf.DataHora as DATE) = d.Data and mf.Desativado = 0 
	and mf.especialidade = e.Especialidade
          
where n.numero <= dbo.ConsultaQuantidadeMaxima(e.idEspecialidade, d.Data)          
and          
(          
          
( mf.idMedico is null )          
          
or          
(          
          
convert(varchar(5),          
case when n.numero <= QuantidadeAteAlmoco then          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - 1 )), dbo.Consulta1HoraInicio(e.idEspecialidade, d.Data))          
else          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - QuantidadeAteAlmoco - 1 )), dbo.ConsultaAlmocofim(e.idEspecialidade, d.data))          
end,108)          
 < convert(varchar(5), mf.Datahora, 108)          
          
or          
          
convert(varchar(5),          
case when n.numero <= QuantidadeAteAlmoco then          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - 1 )), dbo.Consulta1HoraInicio(e.idEspecialidade, d.Data))          
else          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - QuantidadeAteAlmoco - 1 )), dbo.ConsultaAlmocofim(e.idEspecialidade, d.Data))          
end,108)          
          
>= convert(varchar(5), mf.DatahoraFim, 108)          
)          
)          
          
and          
(          
(E.DataPlantao is null          
or d.Data = E.DataPlantao)          
)          
          
and (d.Data >= @DataInicio and d.Data < @DataFinal)     
-- and e.idMedico = 1233  
AND
(
	(e.AgendaQuinzenal = 0 )
	or
	(e.AgendaQuinzenal = 1 and e.DataInicioExibicao = d.data )
	or
	(e.AgendaQuinzenal = 1 and datediff(day, e.DataInicioExibicao, d.data) % 14 = 0 ) 
)
group by d.Data, e.Especialidade)
,
tabvres as (
-- TABVRES = RESERVA DE VAGAS
select          
count(n.numero) as Quantidade,          
d.Data as Data,          
e.Especialidade

from sis_numero n          
join sis_datahora d on cast(d.Data as date) >= @DataInicio
join cad_especialidade e on          
(      
      
 ((          
  isnull(e.DataInicioExibicao, @DataInicio)  <= @DataInicio
  and isnull(e.DataFimExibicao, @DataFinal) >= @DataFinal
  and e.Desativado = 0          
 )          

and (IsNull(e.DataFimExibicao, d.data) >= d.Data)          
          
 )          
 or (e.DataPlantao is not null and e.desativado =0)          
)      
-- reserva      
and      
(      
  ( (datepart(dw, d.data) = 1)    
  and     
  (      
   ((n.numero >= e.DomingoQuantidadeReservaMinimo and n.numero <= e.DomingoQuantidadeReservaMaximo) )      
  ) )      
 or      
  ( (datepart(dw, d.data) = 2)    
  and (      
   ((n.numero >= e.SegundaQuantidadeReservaMinimo and n.numero <= e.SegundaQuantidadeReservaMaximo) )      
  ) )      
 or      
  ( (datepart(dw, d.data) = 3)     
  and (      
   ((n.numero >= e.TercaQuantidadeReservaMinimo and n.numero <= e.TercaQuantidadeReservaMaximo) )      
  ) )     
 or      
  ( (datepart(dw, d.data) = 4)    
  and (      
   ((n.numero >= e.QuartaQuantidadeReservaMinimo and n.numero <= e.QuartaQuantidadeReservaMaximo) )      
  ) )      
 or      
  ( (datepart(dw, d.data) = 5)    
  and (      
   ((n.numero >= e.QuintaQuantidadeReservaMinimo and n.numero <= e.QuintaQuantidadeReservaMaximo) )      
  ) )      
 or      
  ( (datepart(dw, d.data) = 6)    
  and (      
   ((n.numero >= e.SextaQuantidadeReservaMinimo and n.numero <= e.SextaQuantidadeReservaMaximo) )      
  ) )      
 or      
  ( (datepart(dw, d.data) = 7)    
  and (      
   ((n.numero >= e.SabadoQuantidadeReservaMinimo and n.numero <= e.SabadoQuantidadeReservaMaximo) )      
  ) )      
)      
         
join Cad_Medico m on m.idmedico = e.idmedico and m.Desativado =0          
left join vw_Cad_EspecialidadeQuantidadeAteAlmoco a on a.idespecialidade = e.idEspecialidade and a.data is not null and cast( a.data as datetime) = d.Data
left join Cad_MedicoFalta mf on mf.idMedico = m.idMedico 
	and cast(mf.DataHora as DATE) = d.Data and mf.Desativado = 0 
	and mf.especialidade = e.Especialidade
          
where n.numero <= dbo.ConsultaQuantidadeMaxima(e.idEspecialidade, d.Data)          
and          
(          
          
( mf.idMedico is null )          
          
or          
(          
          
convert(varchar(5),          
case when n.numero <= QuantidadeAteAlmoco then          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - 1 )), dbo.Consulta1HoraInicio(e.idEspecialidade, d.Data))          
else          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - QuantidadeAteAlmoco - 1 )), dbo.ConsultaAlmocofim(e.idEspecialidade, d.data))          
end,108)          
 < convert(varchar(5), mf.Datahora, 108)          
          
or          
          
convert(varchar(5),          
case when n.numero <= QuantidadeAteAlmoco then          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - 1 )), dbo.Consulta1HoraInicio(e.idEspecialidade, d.Data))          
else          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - QuantidadeAteAlmoco - 1 )), dbo.ConsultaAlmocofim(e.idEspecialidade, d.Data))          
end,108)          
          
>= convert(varchar(5), mf.DatahoraFim, 108)          
)          
)          
          
          
          
and          
(          
(E.DataPlantao is null          
or d.Data = E.DataPlantao)          
)          
          
and (d.Data >= @DataInicio and d.Data < @DataFinal)     
AND
(
	(e.AgendaQuinzenal = 0 )
	or
	(e.AgendaQuinzenal = 1 and e.DataInicioExibicao = d.data )
	or
	(e.AgendaQuinzenal = 1 and datediff(day, e.DataInicioExibicao, d.data) % 14 = 0 ) 
)
  
group by d.Data, e.Especialidade
),
tabFaltas as (
-- TABFALTAS = FALTA MEDICA
select          
count(n.numero) as Quantidade,          
d.Data as Data,          
e.Especialidade

from sis_numero n          
join sis_datahora d on cast(d.Data as date) >= @DataInicio
join cad_especialidade e on          
(      
      
 ((          
  isnull(e.DataInicioExibicao, @DataInicio)  <= @DataInicio
  and isnull(e.DataFimExibicao, @DataFinal) >= @DataFinal
  and e.Desativado = 0          
 )          
          
and (IsNull(e.DataFimExibicao, d.data) >= d.Data)          
          
 )          
 or (e.DataPlantao is not null and e.desativado =0)          
)      
-- reserva      
and      
(      
  ( (datepart(dw, d.data) = 1))
 or      
  ( (datepart(dw, d.data) = 2))
 or      
  ( (datepart(dw, d.data) = 3))
 or      
  ( (datepart(dw, d.data) = 4))
 or      
  ( (datepart(dw, d.data) = 5))
 or      
  ( (datepart(dw, d.data) = 6))
 or      
  ( (datepart(dw, d.data) = 7))      
)

join Cad_Medico m on m.idmedico = e.idmedico and m.Desativado =0          
left join vw_Cad_EspecialidadeQuantidadeAteAlmoco a on a.idespecialidade = e.idEspecialidade 
	and a.data is not null and cast( a.data as datetime) = d.Data
left join Cad_MedicoFalta mf on mf.idMedico = m.idMedico 
	and cast(mf.DataHora as DATE) = d.Data and mf.Desativado = 0 
	and mf.especialidade = e.Especialidade
          
where n.numero <= dbo.ConsultaQuantidadeMaxima(e.idEspecialidade, d.Data)          
and          
(          
( mf.idMedico is not null )          
or          
(          
          
convert(varchar(5),          
case when n.numero <= QuantidadeAteAlmoco then          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - 1 )), dbo.Consulta1HoraInicio(e.idEspecialidade, d.Data))          
else          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - QuantidadeAteAlmoco - 1 )), dbo.ConsultaAlmocofim(e.idEspecialidade, d.data))          
end,108)          
 < convert(varchar(5), mf.Datahora, 108)          
          
or          
          
convert(varchar(5),          
case when n.numero <= QuantidadeAteAlmoco then          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - 1 )), dbo.Consulta1HoraInicio(e.idEspecialidade, d.Data))          
else          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - QuantidadeAteAlmoco - 1 )), dbo.ConsultaAlmocofim(e.idEspecialidade, d.Data))          
end,108)          
          
>= convert(varchar(5), mf.DatahoraFim, 108)          
)          
)          
          
and          
(          
(E.DataPlantao is null          
or d.Data = E.DataPlantao)          
)          
          
and (d.Data >= @DataInicio and d.Data < @DataFinal)     

AND
(
	(e.AgendaQuinzenal = 0 )
	or
	(e.AgendaQuinzenal = 1 and e.DataInicioExibicao = d.data )
	or
	(e.AgendaQuinzenal = 1 and datediff(day, e.DataInicioExibicao, d.data) % 14 = 0 ) 
)
--and e.Especialidade = 'ANGIOLOGISTA'
group by d.Data, e.Especialidade 
), 
tabvOcup as (
-- TABvOCUP =VAGAS OCUPADAS 
select          
count(n.numero) as Quantidade,          
d.Data as Data,          
e.Especialidade

from sis_numero n          
join sis_datahora d on cast(d.Data as date) >= @DataInicio
join cad_especialidade e on          
(      
      
 ((          
  isnull(e.DataInicioExibicao, @DataFinal)  < @DataFinal
  and isnull(e.DataFimExibicao, @DataInicio) >= @DataInicio
  and e.Desativado = 0          
 )          
          
-- Adicionado em 29/09/2020 para que site e APP respeitem a data final de exibição do médico          
and (IsNull(e.DataFimExibicao, d.data) >= d.Data)          
          
 )          
 or (e.DataPlantao is not null and e.desativado =0)          
)      
-- reserva      
and      
(      
  ( (datepart(dw, d.data) = 1))
 or      
  ( (datepart(dw, d.data) = 2))
 or      
  ( (datepart(dw, d.data) = 3))
 or      
  ( (datepart(dw, d.data) = 4))
 or      
  ( (datepart(dw, d.data) = 5))
 or      
  ( (datepart(dw, d.data) = 6))
 or      
  ( (datepart(dw, d.data) = 7))      
)      

join Cad_Medico m on m.idmedico = e.idmedico and m.Desativado =0          
left join vw_Cad_EspecialidadeQuantidadeAteAlmoco a on a.idespecialidade = e.idEspecialidade and a.data is not null and cast( a.data as datetime) = d.Data          
left join vw_Cad_LancamentoConsulta l on          
  cast(l.dataconsulta as date) = d.Data          
  and l.idmedico = e.idmedico          
  and l.consulta = n.numero      
      
left join vw_Cad_LancamentoConsulta l_idEspec on          
  cast(l_idEspec.dataconsulta as date) = d.Data          
  and l_idEspec.idmedico = e.idmedico          
  and l_idEspec.consulta = n.numero          
  and l_idEspec.idEspecialidade = e.idEspecialidade          
left join Cad_LancamentoServico ls on ls.idLancamento = l.idLancamento and ls.Desistencia = 0          
left join Cad_LancamentoServico lsPlantaoExtra on lsPlantaoExtra.idLancamento = l_idEspec.idLancamento and lsPlantaoExtra.Desistencia = 0          
left join Cad_Servico s on          
 s.idServico =          
 case when e.DataPlantao is null then          
  ls.idServico          
 else          
  lsPlantaoExtra.idServico          
 end          
 and s.Consulta= 1          
left join Cad_MedicoFalta mf on mf.idMedico = m.idMedico 
	and cast(mf.DataHora as DATE) = d.Data and mf.Desativado = 0 
	and mf.especialidade = e.Especialidade
          
where n.numero <= dbo.ConsultaQuantidadeMaxima(e.idEspecialidade, d.Data)          
and          
(          
          
( mf.idMedico is null )          
          
or          
(          
          
convert(varchar(5),          
case when n.numero <= QuantidadeAteAlmoco then          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - 1 )), dbo.Consulta1HoraInicio(e.idEspecialidade, d.Data))          
else          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - QuantidadeAteAlmoco - 1 )), dbo.ConsultaAlmocofim(e.idEspecialidade, d.data))          
end,108)          
 < convert(varchar(5), mf.Datahora, 108)          
          
or          
          
convert(varchar(5),          
case when n.numero <= QuantidadeAteAlmoco then          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - 1 )), dbo.Consulta1HoraInicio(e.idEspecialidade, d.Data))          
else          
 dateadd(MINUTE, ( a.minutosdaconsulta * ( n.numero - QuantidadeAteAlmoco - 1 )), dbo.ConsultaAlmocofim(e.idEspecialidade, d.Data))          
end,108)          
          
>= convert(varchar(5), mf.DatahoraFim, 108)          
)          
)          

and          
(          
(E.DataPlantao is null          
or d.Data = E.DataPlantao)          
)          
          
AND
(
	(e.AgendaQuinzenal = 0 )
	or
	(e.AgendaQuinzenal = 1 and e.DataInicioExibicao = d.data )
	or
	(e.AgendaQuinzenal = 1 and datediff(day, e.DataInicioExibicao, d.data) % 14 = 0 ) 
)
  
and 
(
	(e.DataPlantao is null and l.idLancamento IS not null)
  or
	(e.DataPlantao is not null and l_idEspec.idLancamento IS not null)
)
and (d.Data >= @DataInicio and d.Data < @DataFinal)     
group by d.Data, e.Especialidade
)

select month(dt.Data) as Mes, 
dt.Especialidade, 
SUM(IsNull(t.Quantidade,0)) as [Total de Vagas], 
Sum(IsNull(t3.Quantidade,0)) as [Indisponíveis Por Falta Médica],
SUM(IsNull(t.Quantidade,0))-Sum(IsNull(t3.Quantidade,0)) as [Total de Vagas Disponiveis], 
Sum(IsNull(t2.Quantidade,0)) as [Disponíveis Em Reserva de Vaga],
Sum(IsNull(t4.Quantidade,0)) as [Ocupadas Por Agendamento de Cliente]
from tabDatas dt
left join tabVagas t on cast(t.Data as date) = dt.Data and t.Especialidade = dt.Especialidade
left join tabvres t2 on t2.Data = dt.Data and t2.Especialidade = dt.Especialidade
left join tabFaltas t3 on t3.Data = dt.Data and t3.Especialidade = dt.Especialidade
left join tabvOcup t4 on t4.Data = dt.Data and t4.Especialidade = dt.Especialidade
group by month(dt.Data), dt.Especialidade
order by Mes, dt.Especialidade


