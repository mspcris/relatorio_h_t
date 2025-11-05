Quero ver se este py vai subir para opt


1) Eu não regravo o html, só gero os jsons na pasta em opt e o cron copia para a pasta json em www. 
2) Se a hora é dia 4 14h está absolutamente errada! o cron deveria rodar de hora em hora o export_governanca que gera os jsons. 
3) Existem vários outros processos, por isso o sync_www.sh roda a cada cinco minutos. 

Estou duvidando da informação que aparece no HTML. Para mim ele está olhando para o arquivo errado. 

1) Vamos analisar o cron para ver se tem algum erro neles. 
2) Quem gera a data no json e altera o mtime dele é o script em python.