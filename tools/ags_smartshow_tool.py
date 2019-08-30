# -*- coding: utf-8 -*-
# !/usr/bin/python
__author__ = 'ma_keling'
# Version     : 1.0.0
# Start Time  : 2019-8-7
# Change Log  :
#   2019-8-13: Update methods for merge and publish images
#   2019-8-29 : Update methods for automatic generate vector tile services

import os
import time

# Export all the features to linux local directoy with shapefile format
def copy_data2shapefile_gdb(spark,input_data_url, extent="", field_name='DLBM',workspace='/home/arcgis/export_shps'):
    import uuid
    import geopandas
    from shapely.geometry import MultiPolygon
    from shapely.geometry import Polygon

    def features2shapefile_by_partition(features, fieldname, workspace):
        polygons = []
        labels = []

        shp_name = os.path.join(workspace, str(uuid.uuid1()) + ".shp")

        for feature in features:
            polygon = []
            geo = feature['$geometry']
            rings = geo['rings']
            label_field = int(feature[fieldname])
            labels.append(label_field)

            for ring in rings:
                shapely_ring = Polygon(ring)
                polygon.append(shapely_ring)
            shapely_polygon = MultiPolygon(polygon)
            polygons.append(shapely_polygon)

        df_json = {fieldname: labels, 'geometry': polygons}
        dltb_df = geopandas.GeoDataFrame(data=df_json, geometry=polygons, columns=[fieldname])

        dltb_df.to_file(shp_name)

    if extent == "":
        input_df = spark.read.format("webgis").option("fields", field_name).load(input_data_url)
    else:
        input_df = spark.read.format("webgis").option('extent', extent).option("fields", field_name).load(input_data_url)

    input_df.foreachPartition(lambda features: features2shapefile_by_partition(features, field_name, workspace))

# Loop all the shapfiles in one folder, convert every shapefile to a raster
def convert_Shps2rasters(shp_workspace,images_workspace,field_name, resolution,proj):
    import arcpy

    # convert single feature to raster
    def feature2raster(input_features, shp_workspace, images_workspace, field_name, resolution, proj):
        arcpy.env.overwriteOutput = True
        arcpy.env.workspace = shp_workspace

        # define projection
        sr = arcpy.SpatialReference(proj)
        arcpy.DefineProjection_management(input_features, sr)

        raster_name = os.path.join(images_workspace, input_features[:-4] + ".tif")
        out = arcpy.FeatureToRaster_conversion(input_features, field_name, raster_name, resolution)
        print(out)

    start_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
    start = time.time()
    print("矢量转栅格任务开始，启动时间：" + start_timeStampName)

    arcpy.env.workspace = shp_workspace
    featurelayers = arcpy.ListFiles('*.shp')

    for features in featurelayers:
        print(features)
        feature2raster(features, shp_workspace, images_workspace, field_name, resolution,proj)

    end_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
    end = time.time()
    elapse_time = end - start
    print("矢量转栅格任务结束，结束时间：" + end_timeStampName, "任务总耗时：", elapse_time, "秒")

# Publishing an image dataset to ArcGIS Server as Image Service
def publish_images(draft_workspace,input_raster,con_path,service_name):
    import arcpy
    try:

        start_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
        start = time.time()
        s = start
        print("Publishing Services，Start time：" + start_timeStampName)
        arcpy.env.overwriteOutput = True
        # Set output file names
        sddraft_filename = os.path.join(draft_workspace, service_name + '.sddraft')
        arcpy.CreateImageSDDraft(input_raster, sddraft_filename, service_name, 'ARCGIS_SERVER',
                                     con_path, False, None, "Publish image service for smart show",
                                     "lands,image service")

        end_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
        end = time.time()
        elapse_time = end - start
        print("Create image sddraft finished, at time:" + end_timeStampName, "Elapsed time:", elapse_time, "s")

        # Stage Service
        start = time.time()
        sd_filename = service_name + ".sd"
        sd_output_filename = os.path.join(draft_workspace, sd_filename)
        arcpy.StageService_server(sddraft_filename, sd_output_filename)

        end_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
        end = time.time()
        elapse_time = end - start
        print("Stage Service finished, at time:" + end_timeStampName, "Elapsed time:", elapse_time, "s")

        # Share to portal
        start = time.time()
        arcpy.UploadServiceDefinition_server(sd_output_filename, con_path)
        end_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
        end = time.time()
        elapse_time = end - start
        print("Uploading service definition finished, at time:" + end_timeStampName, "Elapsed time:", elapse_time, "s")

        elapse_time = end - s
        print("Completed publishing images service，ending time：" + end_timeStampName, "Total Elapsed time：", elapse_time, "s")
    except:
        print("Publish images failed.")
        print()
        arcpy.GetMessages()

# Merge images to one
def merge_image(draft_workspace, publish_workspace,raster_gdb, ms_name, prj, origin_rasters_folder):
    import arcpy
    # Create mosaic dataset automatically
    def create_mosaic_dataset(draft_workspace, raster_gdb, ms_name, prj, origin_rasters_folder):
        try:
            start_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
            start = time.time()
            s = start
            print("Start create mosaic dataset：" + start_timeStampName)
            #generate file gdb first
            arcpy.env.overwriteOutput = True

            raster_gdb = arcpy.CreateFileGDB_management(draft_workspace, raster_gdb)

            end_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
            end = time.time()
            elapse_time = end - start
            print("Generate file gdb finished, at time:" + end_timeStampName, "Elapsed time:", elapse_time, "s")
            print("File GDB path:", raster_gdb)

            #create mosaic dataset
            start = time.time()
            out_mosaic_dataset = arcpy.CreateMosaicDataset_management(raster_gdb, ms_name, prj)

            print(out_mosaic_dataset)

            end_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
            end = time.time()
            elapse_time = end - start
            print("Create mosaic dataset finished, at time:" + end_timeStampName, "Elapsed time:", elapse_time, "s")
            print("Mosaic Dataset path:", out_mosaic_dataset)

            start = time.time()

            rasters_origin = os.path.join(draft_workspace,origin_rasters_folder)
            rastype = "Raster Dataset"
            updatecs = "UPDATE_CELL_SIZES"
            updatebnd = "UPDATE_BOUNDARY"
            updateovr = "UPDATE_OVERVIEWS"
            maxlevel = "3"
            maxcs = "#"
            maxdim = "#"
            spatialref = "#"
            inputdatafilter = "*.tif"
            subfolder = "NO_SUBFOLDERS"
            duplicate = "EXCLUDE_DUPLICATES"
            buildpy = "BUILD_PYRAMIDS"
            calcstats = "CALCULATE_STATISTICS"
            buildthumb = "NO_THUMBNAILS"
            comments = "Add Raster Datasets"
            forcesr = "#"
            estimatestats = "ESTIMATE_STATISTICS"
            auxilaryinput = ""
            enablepixcache = "USE_PIXEL_CACHE"
            cachelocation = os.path.join(draft_workspace,'cachelocation')

            arcpy.AddRastersToMosaicDataset_management(
                out_mosaic_dataset, rastype, rasters_origin, updatecs, updatebnd, updateovr,
                maxlevel, maxcs, maxdim, spatialref, inputdatafilter,
                subfolder, duplicate, buildpy, calcstats,
                buildthumb, comments, forcesr, estimatestats,
            auxilaryinput, enablepixcache, cachelocation)

            end_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
            end = time.time()
            elapse_time = end - start
            print("Import rasters to mosaic dataset finished, at time:" + end_timeStampName, "Elapsed time:", elapse_time, "s")

            total_time = end - s
            print("Create mosaic dataset task finished!", "Total elapsed time:", total_time,"s")

            return out_mosaic_dataset

        except:
            print("Create mosaic dataset failed.")
            print()
            arcpy.GetMessages()


    def copyRaster(publish_workspace,mosaic_dataset, ms_name):
        try:
            start_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
            start = time.time()
            s = start
            print("Copy Raster from Mosaic Dataset，Start time：" + start_timeStampName)
            arcpy.env.overwriteOutput = True

            target_image = os.path.join(publish_workspace,ms_name + '.tif')

            #1 Copy Mosaic RasterDataset to Image Dataset with tiff format
            arcpy.CopyRaster_management(mosaic_dataset, target_image)

            end_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
            end = time.time()
            elapse_time = end - start
            print("Copy Raster finished, at time:" + end_timeStampName, "Elapsed time:", elapse_time, "s")

            start = time.time()

            # 2 Build Raster Attribute for image
            arcpy.BuildRasterAttributeTable_management(target_image, "Overwrite")

            end_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
            end = time.time()
            elapse_time = end - start
            print("Build Raster Attribute Table finished, at time:" + end_timeStampName, "Elapsed time:", elapse_time, "s")

            total_time = end - s

            print("Copy Raster Task finished.", "Total Elasped time:", total_time, "s")

            return target_image

        except:
            print("Copy Raster failed.")
            print()
            arcpy.GetMessages()



    start_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
    s0 = time.time()

    print("Merge image task start，Start time：" + start_timeStampName)

    # step 1: create mosaic dataset
    mosaic_dataset = create_mosaic_dataset(draft_workspace,raster_gdb,ms_name,prj,origin_rasters_folder)

    # step 2: copy rasters
    copyRaster(publish_workspace,mosaic_dataset,ms_name)


    end_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
    end = time.time()
    elapse_time = end - s0
    print("Merge image task finished, at time:" + end_timeStampName, "Total Elapsed time:", elapse_time, "s")

# Merge shapfiles to one
def merge_shapefiles(target_workspace,target_shapefile_name):
    import arcpy
    try:
        start_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
        start = time.time()
        print("合并shps任务开始，启动时间：" + start_timeStampName)

        arcpy.env.workspace = target_workspace
        arcpy.env.overwriteOutput = True

        shps = arcpy.ListFiles('*.shp')

        inputs = ""
        target_shp = target_shapefile_name
        for shp in shps:
            inputs += shp + ";"

        inputs = inputs[:-1]

        print("inputs shps:", inputs)
        print("target shp:", target_shp)

        arcpy.Merge_management(inputs, target_shp)

        end_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
        end = time.time()
        elapse_time = end - start
        print("合并shps任务结束，结束时间：" + end_timeStampName, "任务总耗时：", elapse_time, "秒")
    except:
        print("Merge shapefiles failed!")

#Create VTPK
def create_vtpk(aprx,outputPath,vtpkname,schema):
    import arcpy
    try:
        start_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
        start = time.time()
        s = start
        print("Create Vector Tile Package local，Start time：" + start_timeStampName)
        arcpy.env.overwriteOutput = True

        p = arcpy.mp.ArcGISProject(aprx)
        m = p.listMaps()[0]

        print("Packaging " + m.name)

        vptkPath = os.path.join(outputPath,vtpkname + '.vtpk')

        arcpy.CreateVectorTilePackage_management(m, vptkPath, "EXISTING", schema, "INDEXED",
                                             295828763.795777, 564.248588)

        print("VTPK path:", vptkPath)
        end_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
        end = time.time()
        elapse_time = end - start
        print("Create Vector Tile Package finished, at time:" + end_timeStampName, "Elapsed time:", elapse_time, "s")
    except:
        print("Create Vector Tile Package failed.")
        print()
        arcpy.GetMessages()

# Replace aprx datasource
def replace_datasource(aprx, database="",dataset=""):
    import arcpy
    try:
        start_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
        start = time.time()
        print("Update datasource，Start time：" + start_timeStampName)
        arcpy.env.overwriteOutput = True
        p = arcpy.mp.ArcGISProject(aprx)
        m = p.listMaps()[0]
        lyr = m.listLayers()[0]

        conn = lyr.connectionProperties

        if dataset != "":
            conn['dataset'] = dataset
        if database != "":
            conn['connection_info']['database'] = database

        lyr.updateConnectionProperties(lyr.connectionProperties, conn)
        p.save()

        end_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
        end = time.time()
        elapse_time = end - start
        print("Update datasource finished, at time:" + end_timeStampName, "Elapsed time:", elapse_time, "s")

    except:
        print("Update datasource failed.")
        print()
        arcpy.GetMessages()

# publish vector tile services by loop target workspace
def publish_vtpk(portalurl,username,password,target_workspace):
    from arcgis.gis import GIS
    try:
        start_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
        start = time.time()
        print("Publish VTPK to Portal，Start time：" + start_timeStampName)

        gis = GIS(portalurl, username, password)

        vtpks = []
        for root, dirs, files in os.walk(target_workspace):
            for file in files:
                if os.path.splitext(file)[1] == '.vtpk':
                    vtpks.append(os.path.join(root, file))

        for vtpk in vtpks:
            vtpk_path = os.path.join(target_workspace,vtpk)

            print("Publishing VTPK：" ,vtpk_path)

            vtpk_root,vtpk_type = vtpk_path.split(".")
            service_name = vtpk_root.split('/')[-1:][0]

            vtpk_properties = {'title': service_name,
                               'description': 'vtpk layer: ' + service_name,
                               'tags': 'vtpk'}

            vtpk_item = gis.content.add(item_properties=vtpk_properties, data=vtpk_path)

            vtpk_item.publish()

        end_timeStampName = time.strftime('%Y_%m_%d %H:%M:%S', time.localtime(time.time()))
        end = time.time()
        elapse_time = end - start
        print("Publish VTPK to Portal finished, at time:" + end_timeStampName, "Elapsed time:", elapse_time, "s")
    except:
        print("Publish VTPK to Portal failed.")
        print()