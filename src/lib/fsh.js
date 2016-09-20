import _ from 'lodash';
import FSH from './core/FSH';
import fs from 'fs-extra';
import Promise from 'bluebird';

const createInstance = ( defaultConfig = {} ) => {
    const instance = new FSH( defaultConfig );
    // Add aliases
    instance.append = instance.appendFile;
    instance.move = instance.rename;
    return instance; 
};

// Create the default instance to be exported
const fsh = createInstance();

// Factory for creating new instances
fsh.create = defaultConfig => createInstance(defaultConfig);

module.exports = fsh;
