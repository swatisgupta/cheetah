find ./ -name "*.out" | xargs rm -f
find ./ -name "*.err" | xargs rm -f
find ./ -name "*.txt*" | xargs rm -f
find ./ -name "*.bp" | xargs rm -f
find ./ -name "*.bp.dir" | xargs rm -rf
find ./ -name "*.unlock" | xargs rm -rf
find ./ -name "*.dat" | xargs rm -rf
rm -f conf

