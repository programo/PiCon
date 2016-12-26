x=1; for i in *jpg; do counter=$(printf %03d $x); ln -s "$i" /home/aswin/images/"$counter".jpg; x=$(($x+1)); done
