print("Import observations Météo")


# F° HORAIRES Données Meteo France ----

# Les fonctions dont le nom contient '_H' sont celles qui utilisent les données horaires de l'API Meteo Fr puis convertit ces données en valeur quotidienne 

# Fonction cumul données horaires en données jour
hourlyToDaily <- function(data_horaire){
  # data_horaire <- meteo_hor
  data_horaire$jour <- as.Date(data_horaire$aaaammjjhh, format = '%Y%m%d%H')
  
  # agrégation des valeurs
  temp0 <- unique(data_horaire[,c('num_poste', 'nom_usuel')])
  temp1 <- aggregate(data_horaire$rr1, by=list(data_horaire$num_poste, data_horaire$jour), FUN=sum, na.rm=TRUE) # hauteur de precipitations horaire
  # temp2 <- aggregate(data_horaire$TN, by=list(data_horaire$NUM_POSTE, data_horaire$jour), FUN=min, na.rm=TRUE) # temperature minimale sous abri horaire
  # temp3 <- aggregate(data_horaire$TX, by=list(data_horaire$NUM_POSTE, data_horaire$jour), FUN=max, na.rm=TRUE) # temperature maximale sous abri horaire
  # temp4 <- aggregate(data_horaire$TD, by=list(data_horaire$NUM_POSTE, data_horaire$jour), FUN=mean, na.rm=TRUE) # temperature du point de rosée 
  # temp5 <- aggregate(data_horaire$T, by=list(data_horaire$NUM_POSTE, data_horaire$jour), FUN=mean, na.rm=TRUE) # temperature sous abri instantanée
  # temp6 <- aggregate(data_horaire$FF, by=list(data_horaire$NUM_POSTE, data_horaire$jour), FUN=mean, na.rm=TRUE) # force du vent moyenné sur 10 mn, mesurée à  10 m (en m/s et 1/10)
  
  # temp <- merge(temp1, temp2, by=c('Group.1', 'Group.2'))
  # temp <- merge(temp, temp3, by=c('Group.1', 'Group.2'))
  # temp <- merge(temp, temp4, by=c('Group.1', 'Group.2'))
  # temp <- merge(temp, temp5, by=c('Group.1', 'Group.2'))
  # temp <- merge(temp, temp6, by=c('Group.1', 'Group.2'))
  # colnames(temp) <- c('num_poste', 'jour', 'rr', 'tn', 'tx', 'td', 't', 'ff')
  colnames(temp1) <- c('num_poste', 'jour', 'rr')
  data_quot <-  merge(temp0, temp1, by=c('num_poste')) #merge(temp0, temp, by.x=c('num_poste'), by.y =c('num_poste'))
  
  return(data_quot)
}

combineHistMeteo_H <- function(path, filename, stations){
  # path = file.path(lpath$meteofrance, 'donnees_horaires_06')
  # filename = 'meteofr_HorQuot.RData'
  # stations = c('06088001') # , '06075007', '06103002', '06120004'
  
  test<-dir(path = path, pattern = filename)
  if (length(test)>0){
    load(file.path(path,filename))
  } else { # si historique pas deja combiné
    # combine les excel historiques préalablement téléchargés sur data.gouv
    meteo_hor <- list.files(path, full.names = TRUE, pattern = 'H_06') |>  
      map(read.delim, # ne lit que les fichiers dont le nom contient le pattern
          sep = ";",
          colClasses = c("NUM_POSTE" = "character",
                         "AAAAMMJJHH" = "character"),
          .progress = T) |> 
      list_rbind() |> 
      as_tibble(.name_repair = make_clean_names) |>
      mutate(num_poste = str_trim(num_poste)) 
    meteo_hor <- subset(meteo_hor, num_poste %in% stations,
                       select = c('num_poste', 'nom_usuel', 'aaaammjjhh', 'rr1'))
    # cumul par jour
    meteo_horquot <- hourlyToDaily(meteo_hor)
    save(meteo_horquot, file=file.path(path, filename))
  }
  return(meteo_horquot)
}

getMeteoData_H <- function(stations, d1, d2){
  
  # prend l'historique combiné
  load(file.path(lpath$meteofrance,'meteofr_HorQuot.RData'))
  hist <- meteo_horquot
  
  if (d2 >= '2023-01-01'){ 
    # telecharge le dernier excel sur data.gouv
    n_files <- GET("https://www.data.gouv.fr/api/2/datasets/6569b4473bedf2e7abad3b72/") |>
      content() |> 
      pluck("resources", "total")
    files_available <- GET(glue("https://www.data.gouv.fr/api/2/datasets/6569b4473bedf2e7abad3b72/resources/?page=1&page_size={n_files}&type=main")) |> 
      content(as = "text", encoding = "UTF-8") |> 
      fromJSON(flatten = TRUE) |> 
      pluck("data") |> 
      as_tibble(.name_repair = make_clean_names)
    files_available |> 
      mutate(dep = str_extract(title, "(?<=departement_)[:alnum:]{2,3}(?=_)")) |> # extrait le num de dept du nom de fichier
      dplyr::filter(dep == '06', 
             str_detect(title, "2023")) |> 
      pwalk(\(url, title, format, ...) {
        GET(url,
            write_disk(glue(lpath$meteofrance,"/{title}.{format}"), overwrite = TRUE))
      })
    rm(n_files, files_available)
    meteo_tr <- list.files(lpath$meteofrance, full.names = TRUE, pattern = 'HOR') |>  
      map(read.delim, # ne lit que les fichiers dont le nom contient le pattern HOR
          sep = ";",
          colClasses = c("NUM_POSTE" = "character",
                         "AAAAMMJJHH" = "character")) |> 
      list_rbind() |> 
      as_tibble(.name_repair = make_clean_names) |>
      mutate(num_poste = str_trim(num_poste)) 
    
    meteo_tr <- subset(meteo_tr, select = c('num_poste', 'nom_usuel', 'aaaammjjhh', 'rr1'))
    # cumul par jour
    meteo_trquot <- hourlyToDaily(meteo_tr)
    
    # ajoute le dernier telechargement cumulé
    meteo_dfH <- rbind(hist, meteo_trquot)
    meteo_dfH <- meteo_dfH[which(meteo_dfH$num_poste %in% stations & meteo_dfH$jour > d1 & meteo_dfH$jour < d2),] 
  } else {
    meteo_dfH <- hist
    meteo_dfH <- meteo_dfH[which(meteo_dfH$num_poste %in% stations & meteo_dfH$jour > d1 & meteo_dfH$jour < d2),] 
  }

  return(meteo_dfH)
}

# Donnees de references : calcul des cumuls quotidiens de reference sur 1997-2022 
createRefCumul_H <- function(filename, station, d1, d2){
  # est-ce que le fichier existe ?
  test_file<-dir(path = lpath$meteofrance, pattern = paste0(filename,".xlsx"))
  if (length(test_file)==0){ # si le fichier n'existe pas
    # charger les données depuis MeteoFrance
    meteo_hist <- getMeteoData_H(station, d1, d2)
    
    # calcul du cumul journalier pour chaque annee
    meteo_hist$annee <- year(meteo_hist$jour)
    meteo_cum <- NULL
    for (n in unique(meteo_hist$annee)){
      meteo_n <- meteo_hist[meteo_hist$annee == n,]
      meteo_n$num_jour <- yday(meteo_n$jour)
      if (nrow(meteo_n) == 365){ # si annee non bissextile
        meteo_n$num_jour <- ifelse(meteo_n$num_jour <60, meteo_n$num_jour, meteo_n$num_jour+1 )
        meteo_n[nrow(meteo_n) + 1,] <- list(meteo_n$num_poste[1], # on ajoute la ligne 29/02 (yday=60) avec rr = 0
                                            meteo_n$nom_usuel[1],
                                            NA,
                                            0,
                                            meteo_n$annee[1],
                                            60) 
      }
      meteo_n <- meteo_n[order(meteo_n$num_jour),]
      meteo_n$cumul <- cumsum(replace_na(meteo_n$rr, 0))
      meteo_cum <- rbind(meteo_cum, meteo_n)
    }
    
    # calcul des valeurs de ref pour chaque jour
    refcumul <- data.frame()
    temp <- NULL
    for (n in unique(meteo_cum$num_jour)){
      meteo_n <- meteo_cum[meteo_cum$num_jour == n,]
      temp$num_jour <- n
      temp$median <- median(meteo_n$cumul)
      temp$min <- min(meteo_n$cumul)
      temp$min_annee <- max(meteo_n[which(meteo_n$cumul == temp$min),'annee']) # ici je ne garde que l'année la + récente
      temp$max <- max(meteo_n$cumul) 
      temp$max_annee <- max(meteo_n[which(meteo_n$cumul == temp$max),'annee']) # ici je ne garde que l'année la + récente
      temp$Q10 <- quantile(meteo_n$cumul, probs = 0.1, na.rm = T, names=F)
      temp$Q90 <- quantile(meteo_n$cumul, probs = 0.9, na.rm = T, names=F)
      
      refcumul <- rbind(refcumul, temp)
    }
    # enregistrer tableau avec reference
    write.xlsx(refcumul, file = file.path(lpath$meteofrance, paste0(filename,".xlsx")))
  } 
  return(read.xlsx(file.path(lpath$meteofrance, paste0(filename,".xlsx"))))
}

# F° QUOTIDIENNES Données Meteo France ----

getMeteoData <- function(station, d1, d2){
  ## récupère les fichiers csv des données météo france quotidiennes disponibles sur data.gouv 
  ## et filtre avec la station indiquée et les dates données
  if (d2 >= '2023-01-01'){ # pas besoin de télécharger les fichiers précédents car ils ne changent pas
    n_files <- GET("https://www.data.gouv.fr/api/2/datasets/6569b51ae64326786e4e8e1a/") |>
      content() |> 
      pluck("resources", "total")
    files_available <- GET(glue("https://www.data.gouv.fr/api/2/datasets/6569b51ae64326786e4e8e1a/resources/?page=1&page_size={n_files}&type=main")) |> 
      content(as = "text", encoding = "UTF-8") |> 
      fromJSON(flatten = TRUE) |> 
      pluck("data") |> 
      as_tibble(.name_repair = make_clean_names)
    files_available |> 
      mutate(dep = str_extract(title, "(?<=departement_)[:alnum:]{2,3}(?=_)")) |> # extrait le num de dept du nom de fichier
      dplyr::filter(dep == '06', 
             str_detect(title, "RR-T-Vent"),
             str_detect(title, "2023")) |> 
      pwalk(\(url, title, format, ...) {
        GET(url,
            write_disk(glue(lpath$meteofrance,"/{title}.{format}"), overwrite = TRUE))
      })
  }
  ## lecture des fichiers csv en fonction de la date d'interet : ce morceau est très lent
  if (d2 >= '2023-01-01'){
    zippattern <- '2023'
  } else if (d2 >= '1950-01-01'){
    zippattern <- '1950'
  } else {
    zippattern <- '1877'
  }
  meteo_df <- list.files(lpath$meteofrance, full.names = TRUE, pattern = zippattern) |>  
    map(read.delim, # ne lit que les fichiers dont le nom contient le pattern zippattern ?
        sep = ";",
        colClasses = c("NUM_POSTE" = "character",
                       "AAAAMMJJ" = "character")) |> 
    list_rbind() |> 
    as_tibble(.name_repair = make_clean_names) |>
    mutate(num_poste = str_trim(num_poste),
           aaaammjj = ymd(aaaammjj)) 
  
  meteo_df <- subset(meteo_df, num_poste %in% station & aaaammjj >= d1 & aaaammjj <= d2,
                     select = c('num_poste', 'nom_usuel', 'aaaammjj', 'rr'))
  
  return(meteo_df)
}

# Donnees de references : calcul des cumuls quotidiens de reference sur 1997-2022
createRefCumul <- function(filename, station, d1, d2){
  # est-ce que le fichier existe ?
  test_file<-dir(path = lpath$meteofrance, pattern = paste0(filename,".xlsx"))
  if (length(test_file)==0){ # si le fichier n'existe pas
    # charger les données depuis MeteoFrance
    meteo_hist <- getMeteoData(station, d1, d2)
    
    # calcul du cumul journalier pour chaque annee
    meteo_hist$annee <- year(meteo_hist$aaaammjj)
    meteo_cum <- NULL
    for (n in unique(meteo_hist$annee)){
      meteo_n <- meteo_hist[meteo_hist$annee == n,]
      meteo_n$num_jour <- yday(meteo_n$aaaammjj)
      if (nrow(meteo_n) == 365){ # si annee non bissextile
        meteo_n$num_jour <- ifelse(meteo_n$num_jour <60, meteo_n$num_jour, meteo_n$num_jour+1 )
        meteo_n[nrow(meteo_n) + 1,] <- list(meteo_n$num_poste[1], # on ajoute la ligne 29/02 (yday=60) avec rr = 0
                                            meteo_n$nom_usuel[1],
                                            NA,
                                            0,
                                            meteo_n$annee[1],
                                            60) 
      }
      meteo_n <- meteo_n[order(meteo_n$num_jour),]
      meteo_n$cumul <- cumsum(replace_na(meteo_n$rr, 0))
      meteo_cum <- rbind(meteo_cum, meteo_n)
    }
    
    # calcul des valeurs de ref pour chaque jour
    refcumul <- data.frame()
    temp <- NULL
    for (n in unique(meteo_cum$num_jour)){
      meteo_n <- meteo_cum[meteo_cum$num_jour == n,]
      temp$num_jour <- n
      temp$median <- median(meteo_n$cumul)
      temp$min <- min(meteo_n$cumul)
      temp$min_annee <- max(meteo_n[which(meteo_n$cumul == temp$min),'annee']) # ici je ne garde que l'année la + récente
      temp$max <- max(meteo_n$cumul) 
      temp$max_annee <- max(meteo_n[which(meteo_n$cumul == temp$max),'annee']) # ici je ne garde que l'année la + récente
      temp$Q10 <- quantile(meteo_n$cumul, probs = 0.1, na.rm = T, names=F)
      temp$Q90 <- quantile(meteo_n$cumul, probs = 0.9, na.rm = T, names=F)
      
      refcumul <- rbind(refcumul, temp)
    }
    # enregistrer tableau avec reference
    write.xlsx(refcumul, file = file.path(lpath$meteofrance, paste0(filename,".xlsx")))
  } 
  return(read.xlsx(file.path(lpath$meteofrance, paste0(filename,".xlsx"))))
}

# Calcul référence precipitations ----
# chargement donnees historiques
stations <- c('06088001','06075007','06103002','06120004')
meteofr_hist <- combineHistMeteo_H(path = lpath$meteofrance, filename = 'meteofr_HorQuot.RData', stations = stations)

# reference par localisation
# à partir des données horaires
refcumulH_Nice <- createRefCumul_H('refcumulH_Nice', station = '06088001', d1 = '1997-01-01', d2 = '2022-12-31')
refcumulH_StEtienneTin <- createRefCumul_H('refcumulH_StEtienneTin', station = '06120004', d1 = '1997-01-01', d2 = '2022-12-31')
refcumulH_Levens <- createRefCumul_H('refcumulH_Levens', station = '06075007', d1 = '1997-01-01', d2 = '2022-12-31')
#refcumulH_StMartinVes <- createRefCumul_H('refcumulH_StMartinVes', station = '06127001', d1 = '1997-01-01', d2 = '2022-12-31') # pas d'historique horaire pour cette station : prendre l'historique des données quotidiennes


# à partir des données quotidiennes
refcumul_StMartinVes <- createRefCumul('refcumul_StMartinVes', station = '06127001', d1 = '1997-01-01', d2 = '2022-12-31')
refcumul_Nice <- createRefCumul('refcumul_Nice', station = '06088001', d1 = '1997-01-01', d2 = '2022-12-31')
refcumul_StEtienneTin <- createRefCumul('refcumul_StEtienneTin', station = '06120004', d1 = '1997-01-01', d2 = '2022-12-31')
refcumul_Levens <- createRefCumul('refcumul_Levens', station = '06075007', d1 = '1997-01-01', d2 = '2022-12-31')

# F° pas de temps ----

tablmet<-function(station,dt,dat1,dat2){ # données, pas d'aggrégation, date debut du tableau, date fin tableau
  #data0 <- getMeteoData(station = station, d1 = dat1, d2 = Sys.Date())
  data0 <- getMeteoData_H(station = station, d1 = dat1, d2 = Sys.Date())
  # data0$dat2<-floor_date(data0$aaaammjj,unit = dt)
  data0$dat2<-floor_date(data0$jour,unit = dt)
  
  data1<-aggregate(data0$rr,by=list(data0$dat2),FUN=sum,na.rm=TRUE)
  colnames(data1)<-c('Date','Value')
  data1$Date <- as.POSIXct(data1$Date)
  test<-abs(as.numeric(data1$Date)-as.numeric(as.POSIXct(dat1))) # je repere la semaine la plus proche de ma date de départ
  nli<-which.min(test)
  test<-abs(as.numeric(data1$Date)-as.numeric(as.POSIXct(dat2)))
  nli2<-which.min(test)
  
  if(length(nli2)==0){
    print("Attention, données Météo doivent être mises à jour !")
  }
  data2<-data1[nli:nli2,]
  data2$Value<-round(data2$Value,1)
  data2$dat2<-week(data2$Date)
  return(data2)
}

tablcumul<-function(dt,dat1,dat2, normales = normalesNice){
  tbl1<-tablmet('06088001',dt,dat1,dat2)
  tbl2<-tablmet('06075007',dt,dat1,dat2)
  tbl3<-tablmet('06103002',dt,dat1,dat2)
  tbl4<-tablmet('06120004',dt,dat1,dat2)
  
  temp<-merge.data.frame(tbl1,tbl2,by=1,all.x=T,all.y=T)
  temp<-merge.data.frame(temp,tbl3,by=1,all.x=T,all.y=T)
  temp<-merge.data.frame(temp,tbl4,by=1,all.x=T,all.y=T)
  tblcumul<-temp[,c(1:2,4,6,8)]
  
  tblcumul$mois <- month(tblcumul$Date)
  tblcumul <- merge(tblcumul, normales, by='mois', all.x=T)
  tblcumul$mois <- NULL
  tblcumul$Temps <- NULL
  colnames(tblcumul)<-c("Date","Nice","Levens","Saint Martin de Vésubie","Saint Etienne de Tinée", "NormalesNice")
  tblcumul <- tblcumul[order(tblcumul$Date),]
  return(tblcumul)
}


# Calcul cumul obs ----

# Import Normales de Nice mensuelles
normalesNice <- read_excel(file.path(lpath$data, "tdb_ressources.xlsx"), skip = 1, sheet = 'normalesNice')
normalesNice <- normalesNice[-1,c('Temps', 'Pluviométrie (mm)')]
normalesNice$mois <- as.numeric(rownames(normalesNice))

# Import Normales de Levens mensuelles
normalesLev <- read_excel(file.path(lpath$data, "tdb_ressources.xlsx"), skip = 1, sheet = 'normalesLevens')
normalesLev <- normalesLev[-1,c('Temps', 'Pluviométrie (mm)')]
normalesLev$mois <- as.numeric(rownames(normalesLev))

# table des cumuls en fonction du pas de temps 
dat1an <- as.Date(ifelse(is.na(Sys.Date()-years(1)), paste0(year(Sys.Date())-1, '-02-28'), Sys.Date()-years(1))) # gerer les annees bisextiles
print('creation tbl cumul day')
tblcumul_d <- tablcumul("day",dat1an,dat2)
print('creation tbl cumul week')
tblcumul_w <- tablcumul("week",dat1an,dat2)
print('creation tbl cumul month')
tblcumul_m <- tablcumul("month",dat1an,dat2)

rm(dat1an)
print("fin import observations Météo")




## Previsions ----

