Dataquality : 

Le traitement DataQuality permet de lancer des checks de formats de donnée. 
Le traitement se base sur Spark. Les checks sont donc parallele et distribués.

Exemple de configuration de compagne de qualité de donnée : 

```
Pivot = {
  location = {type = "HBase", table = "dco_edma:InterlocuteurClient"},
  fieldsChecks = [
    {
      fieldName = "d:Nom"
      mandatory = true,
      checkes = [
        {id = "1", type = "regex", regex = "\"(?!MAIRE|DEPUTE|PRESIDENT|PDG|GERANT).+\""},
        {id = "2", type = "regex", regex = "\"(?!(\\s)).*"},
        {id = "3", type = "regex", regex = "\"(?!\\s)[A-Za-z\\s-]*\""},
        {id = "4", type = "regex", regex = "\"((?!M.\\s|MR.\\s|MAD.\\s|MME.\\s|MME.\\s|MLE.\\s).)+\""},
        {id = "5", type = "regex", regex = "\".*(?!(A supprimer)).*"},
        {id = "6", type = "regex", regex = "\".*(?!(A qualifier)).*"},
        {id = "7", type = "regex", regex = "\".*(?!(Néant)).*"}
      ]
    }
  ]
}
```

