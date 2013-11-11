Erkl�rungen zum MeteorWebfrontend

1) Start the program
2) Programmstruktur
3) Bugs und Probleme
4) TODO-List
5) Kontaktinformationen

1) Start the program
Das programm starten wir durch die mitgelieferte meteor-webfrontend.sh Datei. F�r einen reibungslosen start ist es erforderlich den Ordner meteor-client/resources in den Resources von stratosphere zu integrieren. Gestartet wird das Shellscript genau wie alle anderen programme auch (source-problem von gestern mittag behoben). Also:
./meteor-webfrontend.sh

2) Programmstruktur
Das Programm wird gestartet die Klasse WebFrontend (mit den selben �bergebenen Variablen-Konventionen wie der Meteor-Client). Hier wird ein Server (meist unter Port 8080 zu erreichen) erstellt und ein Handler hinzugef�gt. 
Der Handler liegt im package eu.stratosphere.meteor.client.web und erzeugt einen ResourceHandler f�r alle statischen Dateien und einen ContextHandler der alle Servlets verwaltet. Au�erdem verf�gt der Handler �ber eine statische update-Methode um die Servlets zu aktualisieren.
Die Seite wird durch 3 Frames aufgebaut.
Links/Oben AnalysisServlet: Nimmt sich die eingabe und schickt sie via update an den ClientFrontend.
Rechts/Oben OutputServlet: Zeigt die JSON-Ausgabe an. Hier liegt auch ein java-script das sich um die Ausgabe der json datei k�mmert. Das javascript syntaxhigh setzt dabei den Text der textarea um.
Unten VisualizationServelt: K�mmert sich um die Visualisierung der Timeline.

Die ClientFrontend klasse k�mmert sich ums einlesen und verwerten des eingegebenen Meteor-Scripts. Es nutzt dabei die Methoden des gegebenen CLClients.

3) Bugs und Probleme
Bisher sollte es ohne Probleme (Fehlermeldung) ein Beispielprogramm durchlaufen k�nnen. Leider sieht man das nur auf der Konsole und nicht im Interface. Das Problem ist das eine gegebene JSON-Datei (getestetes Beispiel mitgeliefert: test.meteor -> 'hdfs://localhost/students2.json') nicht erreichbar ist. Feststellen lie� sich das, indem man im Browser versucht angegebenen Pfad zu erreichen. Er konnte die .json nicht finden und entsprechend griff keines der Javascripts f�r den Output (rechts/oben) und die Visualisierung (unten).
Mehr Probleme sind mir noch nicht aufgefallen.

4) TODO-List
a) Dringend testen ob die javascripts f�r output und visualisierung auch wirklich funktionieren (ich musste anpassungen vornehmen um quellen und felder ein/auszulesen)
b) Analysis-Servlet: isDrawable(String meteorScript). Diese methode soll entscheiden ob ein meteorscript visualisiert werden darf oder nicht
c) l�uft auch lokal nur mit internet Verbindung. (da jQuery die Netzleitung braucht um die javascripts zu importieren)

5) Kontaktinformationen
Wenn etwas wichtiges ist, was Ihr am Programm nicht versteht oder irgendwas nicht klappt oder Ihr sonst wie meine Hilfe braucht k�nnt ihr mir gern eine E-Mail schreiben (wobei ich die selten lesen werde). Falls es dringend ist k�nnt ihr mich auch auf Handy erreichen:
Private E-Mail: andre.greiner-petter@t-online.de
Handy: 0049 171 444 92 92