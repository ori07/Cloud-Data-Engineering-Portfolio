Prioridad,Categoría,Descripción
Alta	Cloud Refactor	Migrar validaciones de rutas en source_validator.py y transforms.py para que utilicen fsspec o gsutil en lugar de librerías locales (os.path, pathlib).
Alta	Resiliencia	Implementar chequeos de existencia de archivos en GCS antes de iniciar el Job (evitar fallos por rutas vacías).
Alta,CI/CD,Implementar Cloud Build Triggers para automatizar el build al hacer merge en main.
Media,Medallion Architecture,Implementar funciones de ingesta para capas Bronze (raw landing) y Silver (técnica/limpieza) para persistencia física.
Media,Testing,"Ampliar cobertura de tests para Edge Cases (archivos vacíos, esquemas malformados, valores nulos extremos)."
Baja,Observabilidad,Integrar Cloud Logging para métricas personalizadas sobre el volumen de datos procesado por el Job.
