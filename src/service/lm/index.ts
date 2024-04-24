import { indexItems } from '../../lib/Item.js';
import logger from '../../lib/Logger.js';
import { CollectionPathParams } from '../../lib/ServiceTypes.js';

import { CollectionProcessingResult, ns, processCollection } from '../util/archivematica.js';
import { cleanup, runTasks } from '../util/index_utils.js';

import { Element, parseXml } from 'libxmljs2';
import { join } from 'path';
import { createItem } from '../../lib/Item.js';
import { Item, MinimalItem } from '../../lib/ItemInterfaces.js';
import { readdirAsync, readFileAsync } from '../../lib/Promisified.js';
import { TextItem } from '../../lib/ServiceTypes.js';

export default async function processDip({ collectionPath }: CollectionPathParams): Promise<void> {
    const customStructMapId = 'structMap_lm';
    try {
        const { rootItem, childItems, textItems } = await processCollection(collectionPath, {
            type: 'custom',
            customStructMapId: customStructMapId,
            isText: (label: string, parents: string[]) => parents[0] === 'transcription',
            getTypeAndLang: (label: string, parents: string[]) => ({
                type: 'translation',
                language: null
            }),
            withRootCustomForFile: (rootCustom: Element, fileId: string) => {
                const orderAttr = rootCustom.get<Element>(`//mets:div[@TYPE="page"]/mets:fptr[@FILEID="${fileId}"]/..`, ns)?.attr('ORDER');
                return {
                    order: orderAttr ? parseInt(orderAttr.value()) : null,
                };
            },
            withRootCustomForText: (rootCustom: Element, fileId: string) => {
                const fptrs = rootCustom.find<Element>(`//mets:div[@TYPE="page"]/mets:fptr[@FILEID="${fileId}"]/../mets:fptr`, ns);
                return fptrs.map(fptrElem => fptrElem.attr('FILEID')?.value() || null).filter(id => id !== null) as string[];
            },
        });

        logger.debug(`Collection ${collectionPath} processed; running cleanup and index`);

        const reassignedItems: CollectionProcessingResult[] = await reassignRootItems(collectionPath, customStructMapId, rootItem, childItems, textItems);

        for (const entry of reassignedItems) {
            await cleanup(entry.rootItem.id);
            await indexItems([entry.rootItem, ...entry.childItems]);
            runTasks(entry.rootItem.id, entry.childItems, entry.textItems);
        }

        logger.debug(`Collection ${collectionPath} indexed; running metadata index, text index and derivative services`);
    }
    catch (e: any) {
        const err = new Error(`Failed to index the collection ${collectionPath}: ${e.message}`);
        err.stack = e.stack;
        throw err;
    }
}


async function reassignRootItems(collectionPath: string, customStructMapId: string, rootItem: Item, childItems: Item[], textItems: TextItem[]): Promise<CollectionProcessingResult[]> {
    const metsFile = (await readdirAsync(collectionPath)).find(file => file.startsWith('METS') && file.endsWith('xml'));
    if (!metsFile) {
        logger.warn(`Could not find a METS file, continuing without restructuring of data.`);
        return [{ rootItem, childItems, textItems }];
    }

    const metsPath = join(collectionPath, metsFile);
    const metsXml = await readFileAsync(metsPath, 'utf8');

    const mets = parseXml(metsXml);

    const rootCustom = mets.get<Element>(`//mets:structMap[@ID="${customStructMapId}"]/mets:div`, ns);

    if (!rootCustom) {
        logger.warn(`Could not find a custom StructMap with id ${customStructMapId}, continuing without restructuring of data.`);
        return [{ rootItem, childItems, textItems }];
    }

    let results: CollectionProcessingResult[] = [];

    const reportNodes = rootCustom.find<Element>(`//mets:div[@TYPE="report"]`, ns);
    if (!reportNodes || !reportNodes.length) {
        logger.warn(`Could not find report-level information, continuing without restructuring of data.`);
        return [{ rootItem, childItems, textItems }];
    }

    for (const reportNode of reportNodes) {
        const id = reportNode.attr('ID')?.value() || null;
        if (!id) {
            throw new Error(`Custom StructMap is missing an ID for a report.`);
        }
        const label = reportNode.attr('LABEL')?.value() || id;

        let selectedChildItems: Item[] = [];
        let selectedTextItems: TextItem[] = [];

        for (const fptrElem of reportNode.find<Element>(`./mets:div[@TYPE="page"]/mets:fptr`, ns)) {
            const fileId = fptrElem.attr('FILEID')?.value() as string;
            const internalId = fileId.substring(5);
            const foundChild = childItems.find(child => child.id == internalId);
            if (foundChild) {
                foundChild.parent_id = id;
                foundChild.collection_id = id;
                selectedChildItems.push(foundChild);
                const index = childItems.indexOf(foundChild);
                if (index > -1) {
                    childItems.splice(index, 1);
                }
            } else {
                const foundText = textItems.find(text => text.id == internalId);
                if (foundText) {
                    foundText.collectionId = id;
                    selectedTextItems.push(foundText);
                    const index = textItems.indexOf(foundText);
                    if (index > -1) {
                        textItems.slice(index, 1);
                    }
                }
            }
        }

        let rootItem = createItem({
            id: id,
            collection_id: id,
            type: 'root',
            label: label
        } as MinimalItem);


        results.push({ rootItem, childItems: selectedChildItems, textItems: selectedTextItems })

    }
    return results;
}